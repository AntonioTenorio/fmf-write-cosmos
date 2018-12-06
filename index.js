require('dotenv').config();
const redis_chalkboard = require('redis');
const redis_heatmap = require('redis');
const Request = require("request");
const DocumentClient = require('documentdb').DocumentClient;
const DocumentBase   = require('documentdb').DocumentBase;
const sql = require('mssql')

const argv = require('yargs')
    .usage('Usage: node $0')
    .example('node $0')
    .argv;

const redisOptionsheatmap = {
    host: process.env.REDIS_HOST_HEATMAP,
    port: process.env.REDIS_PORT_HEATMAP,
    password: process.env.REDIS_PASS_HEATMAP,
};

// Conexion a REDIS
var sub_heatmap = redis_heatmap.createClient(redisOptionsheatmap);

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
/*if(documentdbOptions.host.indexOf('localhost') > 0){
    connectionPolicy.DisableSSLVerification = true;
}*/

// Variables Globales
var stack_players = new Array();
var stack_plays = new Array();

sub_heatmap.monitor(function (err, res) {
    console.log("Entering monitoring mode heatmap.");
});

// Evento monitor de redis nodejs de heatmap
sub_heatmap.on("monitor", function (time, args, raw_reply) {

    // Llenamos los arrays correspondiente al evento que recibimos
    if(args[0].toUpperCase() == 'PUBLISH'){
        var array_args = args[1].split('.');
        var obj_redis = JSON.parse(args[2]);

        var idplayer = stack_players.map(i => i.idPlayer).indexOf(obj_redis.data.idPlayer);
        var obj_player = {
            idPlayer : obj_redis.data.idPlayer,
            idGame : obj_redis.data.idGame,
            idTeam : obj_redis.data.idTeam
        }
        
        if(idplayer == -1){
            stack_players.push(obj_player);
        }

        stack_plays.push(obj_redis.data);
    }
});

//Ejecutar Funciones
setInterval(addDocumentApi, 1000);
setInterval(addHeatmapApi, 10000);

// Revisa si existe un documento en CosmosDB, si no existe crea uno en blanco con el game_id de juego de golstats
function addDocumentApi() {
    var client = new DocumentClient( documentdbOptions.host, { masterKey: documentdbOptions.masterkey }, connectionPolicy);
    if(stack_players.length > 0){

        // Se recorre el array de juegos activos
        stack_players.forEach(function(stack_plays_player) {

            var querySpec = {
                    query:      'select * from coordinates_by_players c where c.id_game = @game_id and c.id_player = @player_id',
                    parameters: [
                            {
                                name:  '@game_id',
                                value: parseInt(stack_plays_player.idGame)
                            },
                            {
                                name:  '@player_id',
                                value: parseInt(stack_plays_player.idPlayer)
                            }
                    ]
            };      

            // Se obtiene el documento basado en los id_game del array de juegos
            client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true }).toArray(function (err, results) {
                if (!err) {
                    if (results.length == 0) {

                        // Si no existe y no hay errores se crea el docuemento en blanco solo con el id_game
                        var obj = {
                            "id_game": stack_plays_player.idGame,
                            "id_player": stack_plays_player.idPlayer,
                            "idTeam": stack_plays_player.idTeam,
                            "header_plays" : ['id', 'momentOfPlayer', 'coordinatesx', 'coordinatesy'],
                            "plays": []
                        };

                        var documentDefinition = obj;
                        client.createDocument(collLink, documentDefinition, function(error, document){
                            if (!err) {
                                console.log('Se crea documento del jugador con id ' + stack_plays_player.idPlayer);
                                
                                // Si no existen errores en el reemplazo del documento se procede a eliminar los registros almacenados en el array de juegos
                                var index_array = stack_players.map(i => i.idPlayer).indexOf(stack_plays_player.idPlayer);
                                if(index_array > -1){
                                    console.log('Se borra el id ' + stack_plays_player.idPlayer + ' del stack de jugadores despues de crear el documento');
                                    stack_players.splice(index_array,1);
                                }
                            }
                        });
                    }else{
                        console.log('Existe el documento del jugador con id ' + stack_plays_player.idPlayer);

                        var index_array = stack_players.map(i => i.idPlayer).indexOf(stack_plays_player.idPlayer);
                        if(index_array > -1){
                            console.log('Se borra el id ' + stack_plays_player.idPlayer + ' del stack de jugadores sin crear el docuemnto porque ya existe');
                            stack_players.splice(index_array,1);
                        }
                    }
                }
            });
        });
    }
}

// Inserta las jugadas de heatmap a los documentos de los juegos activos
function addHeatmapApi() {
    console.log('existe un total de ' + stack_plays.length + ' jugadas en cola');
    var client = new DocumentClient( documentdbOptions.host, { masterKey: documentdbOptions.masterkey }, connectionPolicy);
    var stack_plays_1 = stack_plays.slice();
    var stack_plays_new = new Array();
    
    stack_plays_1.forEach(function(plays){
        if(typeof stack_plays_new[plays.idGame] === 'undefined') stack_plays_new[plays.idGame] = new Array();
        if(typeof stack_plays_new[plays.idGame][plays.idPlayer] === 'undefined') stack_plays_new[plays.idGame][plays.idPlayer] = new Array();

        var new_play = [plays.id, plays.momentOfPlayer, plays.coordinatesx, plays.coordinatesy];
        stack_plays_new[plays.idGame][plays.idPlayer].push(new_play);
    });
    console.log('existe un total de ' + stack_plays_1.length + ' jugadas en cola clon');

    stack_plays_new.forEach(function(games, game_id){
        games.forEach(function(player, player_id){
            var querySpec = {
                    query:      'select * from coordinates_by_players c where c.id_game = @game_id and c.id_player = @player_id',
                    parameters: [
                            {
                                name:  '@game_id',
                                value: parseInt(game_id)
                            },
                            {
                                name:  '@player_id',
                                value: parseInt(player_id)
                            }
                    ]
            };

            client.queryDocuments(collLink, querySpec, { enableCrossPartitionQuery: true }).toArray(function (err, results) {
                if (!err) {
                    var docExists = results[0];

                    if (typeof docExists !== 'undefined'){
                        var documentUrl = collLink + '/docs/' + docExists.id;

                        var new_save_array = docExists.plays.concat(player);
                        docExists.plays = new_save_array;
                        
                        client.replaceDocument(documentUrl, docExists, function (err, result) {
                            if(!err){
                                console.log('registros en stack clon ' + stack_plays_1.length);
                                stack_plays.splice(0, stack_plays_1.length);
                            }
                        });
                    }
                }
            });
        });
    });
}