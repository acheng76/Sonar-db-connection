module.exports = {
    user: 'APPGON_U1_ST', 
    password: 'APPGON_U1_ST5', 
    
    // Thick mode connection string 
    // connectString: '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = RAC-BTLN007000-scan.corp.ads)(PORT = 41521))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = SNRSTsv1)))' 
    
    // Thin mode connection string 
    connectString: 'RAC-BTLN007000-scan.corp.ads:41521/SNRSTsv1' 
}