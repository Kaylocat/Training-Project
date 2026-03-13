CREATE DATABASE IF NOT EXISTS MAL;

USE MAL;

create user if not exists 'admin'@'localhost' identified by '4he7fi3m02ud&';
grant all on MAL.* to 'admin'@'localhost';
flush privileges;
commit;
