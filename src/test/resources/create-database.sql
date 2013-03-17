-- Use this script to setup the mysql database and user that will be used in testing
-- >mysql --user=root
CREATE DATABASE rstaDB ;
CREATE USER 'rstaAdmin'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON rstaDB.* TO 'rstaAdmin'@'localhost' ;

-- >mysql --user=rstaAdmin --password=password rstaDB