// Automating ETL Processes with Node.js
import fs from 'fs';
import { MongoClient } from 'mongodb';
import csvParser from 'csv-parser';
import dotenv from 'dotenv';


dotenv.config();
const csvDataPath = './data/data.csv';

const extractData = (csvDataPath) => {
    return new Promise((resolve, reject) => {
        const data = [];
        fs.createReadStream(csvDataPath)
        .pipe(csvParser())
        .on('data', (item) => {
            data.push(item);
        })
        .on('end', () => {
            resolve(data);
        })
        .on('error', (error) => {
            reject(error)
        });
    });
};

// Transform Phase
const transformDataFunc = (csvData) => {
    return csvData.map((item) => ({
        name: item.NAME.trim(), // Trim whitespace from the name value
        career: item.CAREER.trim()
    }));
}

const dbUrl = process.env.MONGODB_URL;
const connectionString = `${dbUrl}`;
const databaseName = process.env.MONGODB_DATABASE; 
const collectionName = 'users';

//Load data
const loadData = async(transformData, databaseName, collectionName, connectionString) => {
    const client = new MongoClient(connectionString, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      });
    
    try {
        await client.connect();
        console.log('Connected to MongoDB successfully');

        const db = client.db(databaseName);
        const collection = db.collection(collectionName);

        const response = await collection.insertMany(transformData);
        console.log(response, 'Response data');
        console.log(`${response.insertedCount} CSV Data Successfully loaded to MongoDB.`);
    } catch(error){
        console.error('Error Connecting MongoDB', error);
    }

    await client.close();
}

extractData(csvDataPath).then((rawData) => {
    console.log(rawData, 'yes');
    const transformData = transformDataFunc(rawData);
    return loadData(transformData, databaseName, collectionName, connectionString);
}).catch((error) => {
    console.error('Failed to extract or load data into MongoDB:', error);
})

