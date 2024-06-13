const express = require("express");
const { callSoapMethod } = require("../controllers/soapController");

const router = express.Router();

router.post("/clientIdentity", async (req, res) => {
  const { method, args } = req.body;
  try {
    const result = await callSoapMethod(method, args);
    res.json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

module.exports = router;
