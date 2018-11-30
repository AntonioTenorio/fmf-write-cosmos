require('dotenv').config();
const redis_chalkboard = require('redis');
const redis_heatmap = require('redis');
var Request = require("request");
var extend = require('extend');
var DocumentClient = require('documentdb').DocumentClient;
var DocumentBase   = require('documentdb').DocumentBase;

const argv = require('yargs')
    .usage('Usage: node $0')
    .example('node $0')
    .argv;

const redisOptionsChalkboard = {
    host: process.env.REDIS_HOST_CHALKBOARD,
    port: process.env.REDIS_PORT_CHALKBOARD,
    password: process.env.REDIS_PASS_CHALKBOARD,
};

const redisOptionsheatmap = {
    host: process.env.REDIS_HOST_HEATMAP,
    port: process.env.REDIS_PORT_HEATMAP,
    password: process.env.REDIS_PASS_HEATMAP,
};

// Conexion a REDIS
var sub_chalkboard = redis_chalkboard.createClient(redisOptionsChalkboard),
    sub_heatmap = redis_heatmap.createClient(redisOptionsheatmap);

const documentdbOptions = {
    host: process.env.DOCUMENTDB_ENDPOINT,
    masterkey: process.env.DOCUMENTDB_AUTHKEY,
    database: process.env.DOCUMENTDB_DB,
    table: process.env.DOCUMENT_TABLE
};

var databaseId    = documentdbOptions.database,
    collectionId  = documentdbOptions.table,
    dbLink        = 'dbs/' + databaseId,
    collLink      = dbLink + '/colls/' + collectionId;


const connectionPolicy = new DocumentBase.ConnectionPolicy();

// Deshabilita la verificacion SSH si esta en desarrollo
if(documentdbOptions.host.indexOf('localhost') > 0){
    connectionPolicy.DisableSSLVerification = true;
}

// Variables Globales
var games_redis_subscribe = new Array();
var plays_redis_insert = new Array();
var plays_redis_update = new Array();
var heatmap_redis_insert = new Array();


sub_chalkboard.monitor(function (err, res) {
    console.log("Entering monitoring mode chalkboard.");
});

sub_heatmap.monitor(function (err, res) {
    console.log("Entering monitoring mode heatmap.");
});

// Evento monitor de redis nodejs de chalkboard
sub_chalkboard.on("monitor", function (time, args, raw_reply) {
    //Obtengo datos del partido desde api
    /*if(args[0] == 'subscribe'){
        var array_args = args[1].split('.');
        var id_game = array_args[1];

        if(!games_redis_subscribe.includes(id_game)){
            games_redis_subscribe.push(id_game);
        }
    }*/

    // Llenamos los arrays correspondiente al evento que recibimos
    var obj_save = [];
    if(args[0] == 'PUBLISH'){
        var array_args = args[1].split('.');
        var id_game = array_args[1];
    
        if(!games_redis_subscribe.includes(id_game)){
            games_redis_subscribe.push(id_game);
        }

        var array_args = args[1].split('.');
        var id_game = array_args[1];
        var obj_redis = JSON.parse(args[2]);

        var f = new Date();
        var fecha = f.getDate() + "/" + (f.getMonth() +1) + "/" + f.getFullYear();

        console.log("Fecha: " + fecha + ". Tipo de evento: " + obj_redis.event + ". ID de juego: " + id_game);

        if(obj_redis.event == 'NewPlay'){
            plays_redis_insert.push(obj_redis.data);
        }

        if(obj_redis.event == 'UpdatePlay'){
            plays_redis_update.push(obj_redis.data);
        }
    }
});

// Evento monitor de redis nodejs de heatmap
sub_heatmap.on("monitor", function (time, args, raw_reply) {

    //Obtengo datos del partido desde api
    //if(args[0] == 'subscribe'){}

    // Llenamos los arrays correspondiente al evento que recibimos
    var obj_save = [];
    if(args[0] == 'PUBLISH'){
        var array_args = args[1].split('.');
        var id_game = array_args[1];
    
        if(!games_redis_subscribe.includes(id_game)){
            games_redis_subscribe.push(id_game);
        }

        var array_args = args[1].split('.');
        var id_game = array_args[1];
        var obj_redis = JSON.parse(args[2]);

        var f = new Date();
        var fecha = f.getDate() + "/" + (f.getMonth() +1) + "/" + f.getFullYear();

        console.log("Fecha: " + fecha + ". Tipo de evento: " + obj_redis.event + ". ID de juego: " + id_game);

        if(obj_redis.event == 'NewHeatmap'){
            heatmap_redis_insert.push(obj_redis.data);
        }
    }
});

//Ejecutar Funciones
//setInterval(gamesPostmatch, 1000);
setInterval(addDocumentApi, 1000);
setInterval(updateFormationDocumentApi, 1000);
setTimeout(function(){
    setInterval(addPizarraApi, 5000);
    setInterval(updatePizarraApi, 5000);
    setInterval(addHeatmapApi, 5000);
}, 10000);

// Revisa si existe un documento en CosmosDB, si no existe crea uno en blanco con el game_id de juego de golstats
function addDocumentApi() {
    var client = new DocumentClient( documentdbOptions.host, { masterKey: documentdbOptions.masterkey }, connectionPolicy);
    if(games_redis_subscribe.length > 0){

        // Se recorre el array de juegos activos
        games_redis_subscribe.forEach(function(id_game) {
            var querySpec = {
                    query:      'select * from coordinates_by_players c where c.id_game_golstats=@game_id',
                    parameters: [
                            {
                                name:  '@game_id',
                                value: parseInt(id_game)
                            }
                    ] 
            };          

            // Se obtiene el documento basado en los id_game del array de juegos
            client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true }).toArray(function (err, results) {
  
                if (!err) {
                    if (results.length == 0) {

                        // Si no existe y no hay errores se crea el docuemento en blanco solo con el id_game
                        var obj = {
                            "id_game_fmf": 0,
                            "id_game_golstats": parseInt(id_game),
                            "home_team": {},
                            "away_team": {},
                            "plays": [],
                            "plays_modified": [],
                            "plays_heatmap": []
                        };

                        var documentDefinition = obj;
                        client.createDocument(collLink, documentDefinition, function(error, document){}); 
                    }else{

                        var index_game_array = games_redis_subscribe.indexOf(id_game);
                        if(index_game_array >= 0){
                            games_redis_subscribe.splice(index_game_array,1);
                        }
                    }
                }
            }); 
        });
    }
}

// Obtiene los datos desde la API de formación y llena el documento creado que este en blanco en cosmosDB basado en el id de golstats
function updateFormationDocumentApi(){ 
    var client = new DocumentClient( documentdbOptions.host, { masterKey: documentdbOptions.masterkey }, connectionPolicy);
    if(games_redis_subscribe.length > 0){

        // Se recorre el array de juegos activos
        games_redis_subscribe.forEach(function(id_game) {
            var querySpec = {
                    query:      'select * from coordinates_by_players c where c.id_game_golstats=@game_id',
                    parameters: [
                            {
                                name:  '@game_id',
                                value: parseInt(id_game)
                            }
                    ] 
            };         

            // Se obtiene el documento basado en los id_game del array de juegos
            client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true }).toArray(function (err, results) {
                if (!err) {
                    if (results.length > 0) {
                        
                        // Si existe el documento y no existen errores se lee el API de formación y se llena el documento con la información obtenida
                        //Request.get("http://web-services.test/chalkboard-formacion-gls/" + id_game, function (error, response, body){                            
                        Request.get("http://golstats-fmf-services.azurewebsites.net/chalkboard-formacion-gls/" + id_game, function (error, response, body){
                            
                            if(!error){
                                var docExists = results[0];

                                // Si no existen errores en la lectura de la API se actualiza la información del documento
                                if (typeof docExists !== 'undefined'){
                                    var documentUrl = collLink + '/docs/' + docExists.id; 

                                    var api = JSON.parse(body);

                                    docExists.id_game_fmf     = api.id_game_fmf;
                                    docExists.home_team       = api.home_team;
                                    docExists.away_team       = api.away_team;
                                    
                                    // Se reemplaza el documento existente con la información de la API
                                    client.replaceDocument(documentUrl, docExists, function (err, result) {
                                        if (!err) {
                                            
                                            // Si no existen errores en el reemplazo del documento se procede a eliminar los registros almacenados en el array de juegos
                                            var index_game_array = games_redis_subscribe.indexOf(id_game);
                                            if(index_game_array >= 0){
                                                games_redis_subscribe.splice(index_game_array,1);
                                            }
                                        }
                                    }); 
                                }                         
                            }else{
                                console.log(error); 
                            }
                        }); 
                    }                   
                }else{
                    
                    console.log('ERROR!!!! games sin cabeceras ' + id_game);  
                }
            }); 
        });
    }
}

// Inserta las jugadas originales de pizarra a los documentos de los juegos activos
function addPizarraApi() {
    var client = new DocumentClient( documentdbOptions.host, { masterKey: documentdbOptions.masterkey }, connectionPolicy);
    // Se crea una copia del array de jugadas para poder trabajar solo con las jugadas por bloques
    var plays_redis_insert_1 = plays_redis_insert.slice();
    if(plays_redis_insert_1.length > 0){

        // Se crea un arreglo donde se agregan todos los juegos que existen en los datos almacenados en la copia del array de jugadas
        var games = new Array();
        plays_redis_insert_1.forEach(function(values, index){
            if(typeof games[values.idGame] === 'undefined') games[values.idGame] = new Array();
            games[values.idGame].push(values);
        });
        
        // Se recorre el array de juegos creado para proceder a insertar las jugadas
        games.forEach(function(data, game){
            var querySpec = {
                    query:      'select * from coordinates_by_players c where c.id_game_golstats=@game_id',
                    parameters: [
                            {
                                name:  '@game_id',
                                value: parseInt(game)
                            }
                    ] 
            };

            // Se obtiene el documento correspondiente al game almacenado en la copia del array de jugadas
            client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true })
              .toArray(function (err, results) {
                if (!err) {
                    if (results.length > 0) {

                        // Si existe el documento y no existen errores se obtiene la información
                        var docExists = results[0];
                        var documentUrl = collLink + '/docs/' + docExists.id;                        

                        // Se procede a agregar las jugadas que no se encuentrar registradas dentro del objeto de jugadas del documento obtenido
                        data.forEach(function(element){
                            var pos = docExists['plays'].map(i => i.id).indexOf(element.id);

                            if(pos == -1){
                                docExists['plays'].push(element)
                            }
                        });

                        // Se reemplaza el documento con la nueva información
                        client.replaceDocument(documentUrl, docExists, function (err, result) {
                            if (!err) {
                                var querySpec = {
                                        query:      'SELECT c.id FROM coordinates_by_players f JOIN c IN f.plays where f.id_game_golstats=@game_id',
                                        parameters: [
                                                {
                                                    name:  '@game_id',
                                                    value: parseInt(game)
                                                }
                                        ] 
                                };

                                // Se obtienen los id de todas las jugadas que contiene ese documento
                                client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true })
                                .toArray(function (err1, results1) {                                    
                                    if(!err1){
                                        // Basado a la consulta de las jugadas se eliminan las que coincidan del array principal de jugadas
                                        results1.forEach(function(delee_element){
                                            var delete_index = plays_redis_insert.map(i => i.id).indexOf(delee_element.id);
                                            plays_redis_insert.slice(delete_index, 1);
                                        });
                                    }
                                });
                            }
                        });
                    }
                }
            });
        });
    }
}

// Inserta las jugadas modificadas de pizarra a los documentos de los juegos activos
function updatePizarraApi() {
    var client = new DocumentClient( documentdbOptions.host, { masterKey: documentdbOptions.masterkey }, connectionPolicy);
    var plays_redis_update_1 = plays_redis_update.slice();
    if(plays_redis_update_1.length > 0){

        var games = new Array();
        plays_redis_update_1.forEach(function(values, index){
            if(typeof games[values.idGame] === 'undefined') games[values.idGame] = new Array();
            games[values.idGame].push(values);
        });
        
        games.forEach(function(data, game){
            var querySpec = {
                    query:      'select * from coordinates_by_players c where c.id_game_golstats=@game_id',
                    parameters: [
                            {
                                name:  '@game_id',
                                value: parseInt(game)
                            }
                    ] 
            }; 

            client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true })
              .toArray(function (err, results) {
                if (!err) {
                    if (results.length > 0) {
                        var docExists = results[0];
                        var documentUrl = collLink + '/docs/' + docExists.id;                        

                        data.forEach(function(element){
                            var pos = docExists['plays_modified'].map(i => i.id).indexOf(element.id);

                            if(pos == -1){
                                docExists['plays_modified'].push(element)
                            }
                        });

                        client.replaceDocument(documentUrl, docExists, function (err, result) {
                            if (!err) {
                                var querySpec = {
                                        query:      'SELECT c.id FROM coordinates_by_players f JOIN c IN f.plays_modified where f.id_game_golstats=@game_id',
                                        parameters: [
                                                {
                                                    name:  '@game_id',
                                                    value: parseInt(game)
                                                }
                                        ] 
                                };

                                client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true })
                                .toArray(function (err1, results1) {                                    
                                        if(!err1){
                                        results1.forEach(function(delee_element){
                                            var delete_index = plays_redis_update.map(i => i.id).indexOf(delee_element.id);
                                            plays_redis_update.slice(delete_index, 1);
                                        });
                                    }
                                });
                            }
                        });
                    }
                }
            });
        });
    }
}

// Inserta las jugadas de heatmap a los documentos de los juegos activos
function addHeatmapApi() {
    var client = new DocumentClient( documentdbOptions.host, { masterKey: documentdbOptions.masterkey }, connectionPolicy);
    var heatmap_redis_insert_1 = heatmap_redis_insert.slice();
    if(heatmap_redis_insert_1.length > 0){

        var games = new Array();
        heatmap_redis_insert_1.forEach(function(values, index){
            if(typeof games[values.idGame] === 'undefined') games[values.idGame] = new Array();
            games[values.idGame].push(values);
        });
        
        games.forEach(function(data, game){
            var querySpec = {
                    query:      'select * from coordinates_by_players c where c.id_game_golstats=@game_id',
                    parameters: [
                            {
                                name:  '@game_id',
                                value: parseInt(game)
                            }
                    ] 
            }; 

            client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true })
              .toArray(function (err, results) {
                if (!err) {
                    if (results.length > 0) {
                        var docExists = results[0];
                        var documentUrl = collLink + '/docs/' + docExists.id;                        

                        data.forEach(function(element){
                            var pos = docExists['plays_heatmap'].map(i => i.id).indexOf(element.id);

                            if(pos == -1){
                                docExists['plays_heatmap'].push(element)
                            }
                        });

                        client.replaceDocument(documentUrl, docExists, function (err, result) {
                            if (!err) {
                                var querySpec = {
                                        query:      'SELECT c.id FROM coordinates_by_players f JOIN c IN f.plays_heatmap where f.id_game_golstats=@game_id',
                                        parameters: [
                                                {
                                                    name:  '@game_id',
                                                    value: parseInt(game)
                                                }
                                        ] 
                                };

                                client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true })
                                .toArray(function (err1, results1) {                                    
                                    if(!err1){
                                        results1.forEach(function(delee_element){
                                            var delete_index = heatmap_redis_insert.map(i => i.id).indexOf(delee_element.id);
                                            heatmap_redis_insert.slice(delete_index, 1);
                                        });
                                    }
                                });
                            }
                        });
                    }
                }
            });
        });
    }
}

// revisar que los partidos ya se hayan procesado en postmatch
function gamesPostmatch() {

    var games = new Array(50053, 50054, 50055, 50056);

    Request.get("http://servicesfalcondevelopment.us-east-2.elasticbeanstalk.com/management/get/list", function (error, response, body){

    });
    if(games.length > 0){

        games.forEach(function(values, index){
            console.log(values);
        });
    }
}
