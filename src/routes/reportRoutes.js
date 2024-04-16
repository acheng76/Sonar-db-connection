const express = require('express');
const router = express.Router();
const reportController = require('../controllers/reportController');
const avaiReportData = require('../controllers/reportController');

router.get('/perfReportData', reportController.getPerfReportData);
router.get('/avaiReportData', reportController.getAvaiReportData);

module.exports = router;
