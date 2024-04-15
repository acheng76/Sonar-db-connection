const express = require('express');
const router = express.Router();
const reportController = require('../controllers/reportController');

router.get('/perfReportData', reportController.getPerfReportData);

module.exports = router;
