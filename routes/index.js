var express = require('express');
var router = express.Router();

const { esToRedis } = require('../controllers/execute');
// const { allEsToRedis } = require('../controllers/executeAll');
const { esToRedisWithApi } = require('../controllers/executeAllWithApi');

router.get('/execute', esToRedis)
// router.get('/all-execute', allEsToRedis)
router.get('/all-execute-api', esToRedisWithApi)


module.exports = router;
