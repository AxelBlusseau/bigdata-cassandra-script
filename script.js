const cassandra = require('cassandra-driver');
const csv = require('csv-parser')
const fs = require('fs')
const moment = require('moment');

const client = new cassandra.Client({
  contactPoints: ['192.168.99.100'],
  localDataCenter: 'datacenter1',
  keyspace: 'store'
});

const results = [];

fs.createReadStream('pollen_luxembourg.csv')
 .pipe(csv())
 .on('data', (data) => results.push(data))
 .on('end', async () => {
  for(const obj of results){
    obj.NumWeek = moment(obj.Date).format('W');
    const val = Object.values(obj)
    val[0] = '\'' + val[0] + '\'' //Ajouter les quotes pour la construction de la requête
    req = 'INSERT INTO store.test ('+ Object.keys(obj).toString() +') VALUES ('+ val +');'
    await client.execute(req);
  }
  console.log("Done");
});
