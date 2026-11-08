-- Виконується при першому старті MySQL: база для частини 1 і права для користувача
CREATE DATABASE IF NOT EXISTS olympic_dataset;
GRANT ALL PRIVILEGES ON *.* TO 'fp_user'@'%';
FLUSH PRIVILEGES;
