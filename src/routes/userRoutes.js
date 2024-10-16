const express = require('express');
const router = express.Router();
const userController = require('../controllers/userController');

router.get('/userCustList', userController.getUserCustList);
router.get('/availAccess', userController.getAvailAccess);
router.get('/availCustomerGroup', userController.getAvailCustomerGroup);

module.exports = router;
