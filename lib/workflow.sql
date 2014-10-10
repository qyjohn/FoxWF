DROP TABLE IF EXISTS `jobs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobs` (
  `workflow` varchar(250) NOT NULL,
  `id` varchar(80) NOT NULL,
  `name` varchar(80) NOT NULL,
  `started` datetime NOT NULL,
  `completed` datetime NOT NULL,
  `worker` varchar(250) NOT NULL,
  `status` varchar(80) NOT NULL,
  KEY `workflow` (`workflow`),
  KEY `id` (`id`),
  KEY `name` (`name`),
  KEY `started` (`started`),
  KEY `completed` (`completed`),
  KEY `worker` (`worker`),
  KEY `status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `workflow`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `workflow` (
  `uuid` varchar(250) NOT NULL,
  `name` varchar(250) NOT NULL,
  `directory` varchar(250) NOT NULL,
  `submitted` datetime NOT NULL,
  `updated` datetime NOT NULL,
  `status` varchar(20) NOT NULL,
  KEY `uuid` (`uuid`),
  KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

