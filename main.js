const fs = require('fs');
const papa = require('papaparse');
const AWS = require('aws-sdk');
const file = fs.createReadStream('inventario.csv');

// Usar perfil de configuracion, por defecto se usa "default"
const credentials = new AWS.SharedIniFileCredentials({ profile: 'workana' });
AWS.config.update({ region: 'us-east-2', credentials });

const ddb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });

const TableName = "inventario";

// Parsea csv a json
papa.parse(file, {
    header: true,
    skipEmptyLines: true,
    complete: onComplete
});

function onComplete(results) {
    const array = results.data;
    const dynamoItems = array.map(item => {
        // Necesitamos obtener los valores y llaves de cada item,
        // para luego identifar el tipo de dato del valor y crear el objeto
        // con el schema de Dynamo: { PutRequest: { Item: dynamoSchema } }
        let dynamoSchema = Object.entries(item).reduce((acc, [key, value]) => {
            let type = isNaN(value) ? 'S' : 'N';
            let cleanKey = key.replace(/\s/g, '');
            return ({
                ...acc, [cleanKey]: {
                    [type]: value
                }
            })
        }, {})
        return { PutRequest: { Item: dynamoSchema } }
    })

    const chunks = chunkArray(dynamoItems, 25);
    console.log('=== chunks size ===', chunks.length)
    chunks.forEach(chunk => {
        try {
            const dynamoData = { [TableName]: chunk }
            const params = { RequestItems: dynamoData }
            write2dynamo(params)
        } catch (error) {
            console.log("Error: ", error)
        }
    })
}

// Parte el array en chunks de tamaño 'size'
function chunkArray(arr, size) {
    return Array.from({ length: Math.ceil(arr.length / size) }, (v, i) =>
        arr.slice(i * size, i * size + size)
    );
}

// Promisifica la escritura en DynamoDB
function write2dynamo(params) {
    return new Promise((resolve, reject) => {
        ddb.batchWriteItem(params, function (err, data) {
            if (err) {
                return reject(err);
            } else {
                console.log("Success", data);
                return resolve(data);
            }
        });
    })
}