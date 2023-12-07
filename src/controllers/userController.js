// controllers/userController.js
const oracledb = require('oracledb');
const sonarDBConfig = require('../config/sonarDBConfig');
const fs = require('fs');

async function getUserCustList(req, res) {
    try { 
      const connection = await oracledb.getConnection(sonarDBConfig);
      console.log('SONAR database connected');

      const userID = req.query.userID;
      const sql = fs.readFileSync('./src/query/userCustList.sql', 'UTF-8').toString();;

      const queryResult = await connection.execute(sql, {userID: userID});

      const desiredColumns = ['CUST_CD', 'CUST_DESC_TXT' ];
      const transformedData = queryResult.rows.map(row => {
        const rowObject = {};
        queryResult.metaData.forEach((column, index) => {
          if (desiredColumns.includes(column.name)) {
            rowObject[column.name] = row[index];
          }
        });
        return rowObject;
      });
  
      res.json(transformedData);
    } catch (error) {
      console.error(error);
      res.status(500).send('Internal Server Error');
    }
  }

module.exports = {
  getUserCustList,
};
