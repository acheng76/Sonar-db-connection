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

  async function getAvailAccess(req, res) {
    try { 
      const connection = await oracledb.getConnection(sonarDBConfig);
      console.log('SONAR database connected to get-Avail-Access');

      const userID = req.query.userID;
      const sql = fs.readFileSync('./src/query/availAccess.sql', 'UTF-8').toString();;

      const queryResult = await connection.execute(sql);

      // const desiredColumns = ['ACC_ROLE_EXTRNL_NM', 'ACCESS_ROLE_INTERNAL_NM' ];
      // const transformedData = queryResult.rows.map(row => {
      //   const rowObject = {};
      //   queryResult.metaData.forEach((column, index) => {
      //     if (desiredColumns.includes(column.name)) {
      //       rowObject[column.name] = row[index];
      //     }
      //   });
      //   return rowObject;
      // });
  
    //   res.json(transformedData);
    // } catch (error) {
    //   console.error(error);
    //   res.status(500).send('Internal Server Error');
    // }
    const transformedData = queryResult.rows.map(row => {
      const rowObject = {};
      queryResult.metaData.forEach((column, index) => {
        rowObject[column.name] = row[index];
      });
      return rowObject;
    });

    res.json(transformedData);
  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error');
  }
}
async function getAvailCustomerGroup(req, res) {
  try { 
    const connection = await oracledb.getConnection(sonarDBConfig);
    console.log('SONAR database connected to avail-Cutomer-Group');
    const appuserID = req.query.appuserID;
    const sql = fs.readFileSync('./src/query/availCustomerGroup.sql', 'UTF-8').toString();;

    //const queryResult = await connection.execute(sql);
    const queryResult = await connection.execute(sql, {appuserID: appuserID});

  const transformedData = queryResult.rows.map(row => {
    const rowObject = {};
    queryResult.metaData.forEach((column, index) => {
      rowObject[column.name] = row[index];
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
  getAvailAccess,
  getAvailCustomerGroup,
};