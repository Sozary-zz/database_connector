#!/usr/bin/env node
 // ======================================================
// => DEPENDENCIES
// ======================================================
const net = require('net');
const axios = require('axios');
const mysql = require('mysql');
const pg = require('pg')
const mssql = require('mssql')
// ======================================================

// ======================================================
// => VARIABLES
// ======================================================
const kizeo_api_uri = "https://www.kizeoforms.com:443/rest/v3/"
const default_port = 59476
var tokens = {}
var server = null
var databases_handler = null
var connections = []
var axiosInstance = axios.create({
  baseURL: kizeo_api_uri,
  timeout: 60000,
  headers: {
    Authorization: null
  }
});
// ======================================================

// ======================================================
// => CLASSES
// ======================================================
class DatabasesHandler {
  constructor() {
    this.databases = {}
  }
  /**
   * Store databases.
   *
   * @param {string} db_identifier
   *   mysql, postgressql or microsoftsql.
   * @param {any} db_data
   *   Instance of connection for this database.
   */
  addDB(db_identifier, db_data) {
    this.databases[db_identifier] = db_data
  }

  injectForm(form) {
    Object.keys(this.databases).forEach(key => {
      this.databases[key].form = form
    })
  }

  insertData(field, key) {
    Object.keys(this.databases).forEach(key => {
      this.databases[key].insertData(field, key)
    })
  }
  updateRequest(number_data) {
    Object.keys(this.databases).forEach(key => {
      this.databases[key].updateRequest(number_data)
    })
  }
  send(err, number_data) {
    Object.keys(this.databases).forEach(async (key) => {
      try {
        await this.databases[key].send(err, number_data)
      } catch (error) {
        // console.log(error);

      }
    })
  }
}

class Connection {
  constructor(connection) {
    this.connection = connection
    this.id = "client-" + connections.length
  }

  close() {
    this.connection.end()
    for (let i = 0; i < connections.length; i++)
      if (connections[i].id === this.id)
        connections.splice(i, 1)
  }
}

class PostgresSender {
  constructor(host, user, password, database, port) {
    this.postgres_connection = new pg.Pool({
      host: host,
      user: user,
      password: password,
      database: database,
      port: port,
    })
    this.data = {}
    this.form = null
    this.queries = ""
    this.prepared = []
  }

  insertData(data, data_id) {
    this.data[data_id] = this.data[data_id] ? this.data[data_id] : []
    switch (data.value.type) {
      case "select":
      case "schema":
      case "barcode":
      case "contact":
      case "tagnfc":
      case "reference":
      case "calculation":
      case "datetime":
      case "choice":
      case "adresse":
      case "zone_de_texte":
      case "section":
      case "audio":
      case "text":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: data.value.value,
          name: data.field
        })
        break;
      case "fixed_text":
        this.data[data_id].push({
          type: "VARCHAR(1000)",
          value: data.value.value,
          name: data.field
        })
        break;
      case "slider":
      case "counter":
        this.data[data_id].push({
          type: "int",
          value: parseInt(data.value.value),
          name: data.field
        })
        break;
      case "checkbox":
        this.data[data_id].push({
          type: "bit",
          value: data.value.boolean ? 1 : 0,
          name: data.field
        })
        break;
      case "paint":
      case "signature":
      case "photo":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/rest/v3/forms/${this.form.id}/data/${data_id}/medias/${data.value.value}`,
          name: data.field
        })
        break;
      case "subform":
        data.value.value.forEach(value => {
          Object.keys(value).forEach(key => {
            this.insertData(value[key]);
          });
        })
        break;
      case "fixed_image":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/rest/v3/forms/${this.form.id}/medias/${data.value.value}`,
          name: data.field
        })

        break
      case "fixed_attached":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/process/get_form_fileV2.php?filename=${data.value.value}`,
          name: data.field
        })
        break;
      case "attached":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/data/medias/${this.form.id}/${data_id}/${data.value.value}`,
          name: data.field
        })
        break;

      case "geoloc":
        this.data[data_id].push({
          type: "float",
          value: parseFloat(data.value.value),
          name: data.field
        })
        break
      default:
        break;
    }

  }

  updateRequest(number_data) {
    let keys = Object.keys(this.data)
    this.dropTable()
    if (keys.length >= 1)
      this.createTable(this.data[keys[0]], number_data)
    for (const k of keys) {
      this.insertSQLData(this.data[k], number_data)
    }
    this.data = {}
  }


  send(err_f) {
    return new Promise((resolve, reject) => {

      this.chunkQueries(this.queries.split(";"), this.prepared, () => {
        this.queries = ""
        this.prepared = []
        this.postgres_connection.end()
        resolve()
      }, () => {
        reject(new Error(400))

      })
    })
  }
  chunkQueries(queries, prepared, callback, error) {
    if (queries[0] != '') {
      let splitted = queries[0].split("$")
      let to_prepare = splitted.length - 1
      let final_query = splitted[0]
      let final_prepare = []

      for (let i = 0; i < to_prepare; i++) {
        final_query += "$" + (i + 1) + splitted[i + 1].replace(/\d*/g, "")
        final_prepare.push(prepared.shift())
      }

      this.postgres_connection.query(final_query, final_prepare == [] ? undefined : final_prepare, (err, r) => {
        if (err)
          error()
        queries.shift()
        this.chunkQueries(queries, prepared, callback, error)
      })
    } else {
      callback()
    }
  }
  insertSQLData(data, number_data) {
    let begin = 'INSERT INTO \"' + this.form.id + '\" ('
    let to_add = ""
    let arr = []

    for (let i = 0; i < data.length / number_data; i++) {
      begin += data[i].name + (i != (data.length / number_data) - 1 ? "," : "")
    }
    for (let j = 0, k = 0; j < number_data; j++) {
      for (let i = 0; i < (data.length / number_data); i++) {
        to_add += `$${(k+1)},`
        arr.push(data[k++].value)
      }
      to_add = to_add.substring(0, to_add.length - 1);
      this.queries += begin + ") VALUES (" + to_add + ");"
      this.prepared = this.prepared.concat(arr)
      to_add = ""
      arr = []

    }
  }

  dropTable() {
    this.queries += 'DROP TABLE IF EXISTS \"' + this.form.id + "\";"
  }

  createTable(selected_data, number_data) {
    this.queries += 'CREATE TABLE \"' + this.form.id + '\" (id serial PRIMARY KEY'
    for (let i = 0; i < selected_data.length / number_data; i++)
      this.queries += "," + selected_data[i].name + " " + selected_data[i].type
    this.queries += ");"
  }
}

class MySQLSender {
  constructor(host, user, password, database, port) {
    this.mysql_connection = mysql.createConnection({
      multipleStatements: true,
      host: host,
      user: user,
      password: password,
      database: database,
      port: port,
    })
    this.data = {}
    this.form = null
    this.queries = ""
    this.prepared = []
  }

  insertData(data, data_id) {
    this.data[data_id] = this.data[data_id] ? this.data[data_id] : []
    switch (data.value.type) {
      case "select":
      case "schema":
      case "barcode":
      case "contact":
      case "tagnfc":
      case "reference":
      case "calculation":
      case "datetime":
      case "choice":
      case "adresse":
      case "zone_de_texte":
      case "section":
      case "audio":
      case "text":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: data.value.value,
          name: data.field
        })
        break;
      case "fixed_text":
        this.data[data_id].push({
          type: "VARCHAR(1000)",
          value: data.value.value,
          name: data.field
        })
        break;
      case "slider":
      case "counter":
        this.data[data_id].push({
          type: "int",
          value: parseInt(data.value.value),
          name: data.field
        })
        break;
      case "checkbox":
        this.data[data_id].push({
          type: "bit",
          value: data.value.boolean ? 1 : 0,
          name: data.field
        })
        break;
      case "paint":
      case "signature":
      case "photo":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/rest/v3/forms/${this.form.id}/data/${data_id}/medias/${data.value.value}`,
          name: data.field
        })
        break;
      case "subform":
        data.value.value.forEach(value => {
          Object.keys(value).forEach(key => {
            this.insertData(value[key]);
          });
        })
        break;
      case "fixed_image":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/rest/v3/forms/${this.form.id}/medias/${data.value.value}`,
          name: data.field
        })

        break
      case "fixed_attached":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/process/get_form_fileV2.php?filename=${data.value.value}`,
          name: data.field
        })
        break;
      case "attached":
        this.data[data_id].push({
          type: "VARCHAR(255)",
          value: `https://www.kizeoforms.com/data/medias/${this.form.id}/${data_id}/${data.value.value}`,
          name: data.field
        })
        break;

      case "geoloc":
        this.data[data_id].push({
          type: "float",
          value: parseFloat(data.value.value),
          name: data.field
        })
        break
      default:
        break;
    }

  }

  updateRequest(number_data) {
    let keys = Object.keys(this.data)
    this.dropTable()
    if (keys.length >= 1)
      this.createTable(this.data[keys[0]], number_data)
    for (const k of keys) {
      this.insertSQLData(this.data[k], number_data)
    }
    this.data = {}
  }

  send(err_f) {
    return new Promise((resolve, reject) => {
      this.mysql_connection.connect((err) => {
        if (err) err_f()
        else {
          this.mysql_connection.query(this.queries, this.prepared, (err, results) => {
            if (err) {
              reject(new Error(400))
            } else {
              this.queries = ""
              this.prepared = []
              this.mysql_connection.end()
              resolve()
            }
          })
        }
      });
    })
  }

  insertSQLData(data, number_data) {
    let begin = 'INSERT INTO `' + this.form.id + '` ('
    let to_add = ""
    let arr = []

    for (let i = 0; i < data.length / number_data; i++) {
      begin += data[i].name + (i != (data.length / number_data) - 1 ? "," : "")
    }
    for (let j = 0, k = 0; j < number_data; j++) {
      for (let i = 0; i < (data.length / number_data); i++) {
        to_add += "?,"
        arr.push(data[k++].value)
      }
      to_add = to_add.substring(0, to_add.length - 1);
      this.queries += begin + ") VALUES (" + to_add + ");"
      this.prepared = this.prepared.concat(arr)
      to_add = ""
      arr = []

    }
  }

  dropTable() {
    this.queries += 'DROP TABLE IF EXISTS `' + this.form.id + "`;"
  }

  createTable(selected_data, number_data) {
    this.queries += 'CREATE TABLE `' + this.form.id + '` (id INT AUTO_INCREMENT PRIMARY KEY'
    for (let i = 0; i < selected_data.length / number_data; i++)
      this.queries += "," + selected_data[i].name + " " + selected_data[i].type
    this.queries += ");"
  }
}

// ======================================================


// ======================================================
// => SERVER
// ======================================================
server = net.createServer(connection => {
  console.log('client connected');
  let new_connection = new Connection(connection)
  connections.push(new_connection)

  connection.on('end', () => {
    console.log('client disconnected');
    new_connection.close()
  });

  connection.on("data", (data) => {
    data = JSON.parse(data)
    switch (data.command) {
      case "login":
        login(data.args, new_connection, response => {
          connection.write(JSON.stringify({
            command: "login",
            status: response ? 200 : 400
          }))
        })
        break

      case "getForms":
        getForms(new_connection, data => {
          if (data) {
            connection.write(JSON.stringify({
              command: "getForms",
              status: 200,
              forms: data
            }))
          } else {
            connection.write(JSON.stringify({
              command: "getForms",
              status: 400,
            }))
          }
        })
        break;

      case "exportForms":
        connection.write(JSON.stringify({
          command: "exportForms",
          status: 200,
        }))
        break

      case "retrieveForm":
        retrieveForm(new_connection, data.data, data => {
          connection.write(JSON.stringify({
            command: "retrieveForm",
            status: 200,
            data: Object.keys(data)
          }))
        });
        break;

      case "finalStep":
        connection.write(JSON.stringify({
          command: "finalStep",
          status: 200,
        }))
        break

      case "selectedDB":
        databases_handler = new DatabasesHandler()
        Object.keys(data.db).forEach(k => {
          let key = k.toLowerCase()
          let _db_
          switch (key) {
            case "mysql":
              _db_ = new MySQLSender(data.db[k].host, data.db[k].user, data.db[k].password, data.db[k].db_name, data.db[k].port)
              break;
            case "postgressql":
              _db_ = new PostgresSender(data.db[k].host, data.db[k].user, data.db[k].password, data.db[k].db_name, data.db[k].port)
              break
          }
          databases_handler.addDB(key, _db_)
        })

        sendAllData(Object.entries(data.forms), databases_handler, new_connection, () => {
          connection.write(JSON.stringify({
            command: "end",
            status: 200,
          }))
        }, () => {
          connection.write(JSON.stringify({
            command: "end",
            status: 400,
          }))
        })
        break;
    }
  })
})
server.listen(process.argv[2] ? process.argv[2] : default_port, () => {
  console.log('server is listening');
});
// ======================================================


// ======================================================
// => FUNCTIONS
// ======================================================

// Function that checks the args from the client
function argsValidator(args, rules) {
  for (var rule of rules)
    if (!args.hasOwnProperty(rule))
      return false
  return true
}

// Function that handles the login check
function login(data, connection, callback) {
  if (argsValidator(data, ["user", "password", "company"])) {
    axiosInstance
      .post("login", {
        user: data.user,
        password: data.password,
        company: data.company
      })
      .then(result => {
        if (result.status === 200) {
          tokens[connection.id] = result.data.data.token;
          callback(true)
        } else callback(false)
      })
      .catch(() => {
        callback(false)
      });
  } else {
    callback(false)
  }
}

// Function that retrieves all the forms
function getForms(connection, callback) {
  axiosInstance
    .get("forms", {
      headers: {
        Authorization: tokens[connection.id]
      }
    })
    .then(result => {
      if (result.status === 200) {
        callback(result.data.forms)
      } else
        callback(false)
    })
    .catch(() => {
      callback(false)
    });
}

// Function that gets field of a peculiar form
function retrieveForm(connection, form_id, callback) {
  axiosInstance
    .get("forms/" + form_id, {
      headers: {
        Authorization: tokens[connection.id]
      }
    })
    .then(result => {
      if (result.status === 200) {
        callback(result.data.form.fields)
      } else
        callback(false)
    })
    .catch(() => {
      callback(false)
    });
}

// Function that retrieves form's data
function getDataList(form_id, connection, fields, callback) {
  axiosInstance
    .get("forms/" + form_id + "/data/all", {
      headers: {
        Authorization: tokens[connection.id]
      }
    })
    .then(result => {
      if (result.status === 200) {
        getDataByField(result.data.data, form_id, connection, fields, callback)
      } else {
        callback(false)
      }
    })
    .catch(() => {
      callback(false)
    });
}

// Function that retrieves data's content by specifying the field
function getDataByField(data, form_id, connection, fields, callback, iterator = 0, res = {}) {
  if (iterator < data.length) {
    axiosInstance
      .get("forms/" + form_id + "/data/" + data[iterator].id, {
        headers: {
          Authorization: tokens[connection.id]
        }
      })
      .then(result => {
        if (result.status === 200) {
          for (const [key, value] of Object.entries(result.data.data.fields)) {
            if (fields.some(field => {
                return key.includes(field) || fields.includes("all")
              })) {
              res[data[iterator].id] = res[data[iterator].id] ? res[data[iterator].id] : []
              res[data[iterator].id].push({
                field: key,
                value: value
              })
            }
          }
          getDataByField(data, form_id, connection, fields, callback, iterator + 1, res)
        } else {
          callback(false)
        }
      })
      .catch((d) => {

        callback(false)
      });
  } else {
    callback(res)
  }
}

// Function that handles the sending of all data over the databases
function sendAllData(forms, db_handler, connection, callback, err_f, iterator = 0) {
  if (iterator < forms.length) {
    getDataList(forms[iterator][1].id, connection, forms[iterator][1].fields, result => {
      db_handler.injectForm(forms[iterator][1])

      let keys = Object.keys(result)

      keys.forEach(key => {
        result[key].forEach(field => {
          db_handler.insertData(field, key)
        })
      });
      db_handler.updateRequest(keys.length)
      sendAllData(forms, db_handler, connection, callback, err_f, iterator + 1)
    })
  } else {
    db_handler.send(err_f)
    callback()
  }
}
// ======================================================
