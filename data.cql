-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
CREATE TABLE IF NOT EXISTS store.test (Date Date PRIMARY KEY,Ambrosia int,Artemisia int,Asteraceae int,Alnus int,Betula int,Ericaceae int,Carpinus int,Castanea int,Quercus int,Chenopodium int,Cupressaceae int,Acer int,Fraxinus int,Gramineae int,Fagus int,Juncaceae int,Aesculus int,Larix int,Corylus int,Juglans int,Umbellifereae int,Ulmus int,Urtica int,Rumex int,Populus int,Pinaceae int,Plantago int,Platanus int,Salix int,Cyperaceae int,Filipendula int,Sambucus int,Tilia int,MaxAirTempC float,MinAirTempC float,PrecipitationC float,NumWeek int);