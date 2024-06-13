const soap = require("soap");
const path = require("path");

// const wsdlUrl = path.join(__dirname, '../path/to/your/ClientIdentityProfileService.wsdl');
const wsdlUrl = "./wsdl/ClientIdentityProfileService_v2_0.wsdl";

const createSoapClient = async () => {
  return new Promise((resolve, reject) => {
    soap.createClient(wsdlUrl, (err, client) => {
      if (err) {
        reject(err);
      } else {
        resolve(client);
      }
    });
  });
};

const callSoapMethod = async (method, args) => {
  try {
    const client = await createSoapClient();
    return new Promise((resolve, reject) => {
      client[method](args, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  } catch (error) {
    throw new Error(`Error calling SOAP method: ${error.message}`);
  }
};

module.exports = {
  callSoapMethod,
};
