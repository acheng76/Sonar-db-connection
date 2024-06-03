const express = require("express");
const router = express.Router();
const reportController = require("../controllers/reportController");

router.get("/perfReportData", reportController.getPerfReportData);
router.get("/avaiReportData", reportController.getAvaiReportData);
router.get("/incidentData", reportController.getincidentData);
router.get("/perfCoreMMPReportData", reportController.getPerfCoreMMPReportData);
router.get(
  "/perfCoreMMPReportDataTest",
  reportController.getPerfCoreMMPReportDataTest
);

module.exports = router;
