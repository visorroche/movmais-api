require('dotenv').config();

import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { Users } from './entities/Users';
import { Companies } from './entities/Companies';
import { Groups } from './entities/Groups';
import { Platforms } from './entities/Platforms';
import { CompanyPlatforms } from './entities/CompanyPlatforms';
import { CompanyUsers } from './entities/CompanyUsers';
import { Customers } from './entities/Customers';
import { Orders } from './entities/Orders';
import { OrderItems } from './entities/OrderItems';
import { Products } from './entities/Products';
import { Logs } from './entities/Logs';

export const AppDataSource = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: +(process.env.DB_PORT || 5432),
  username: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASS || 'postgres',
  database: process.env.DB_NAME || 'movmais',
  // IMPORTANTE:
  // Não habilite synchronize em banco com dados/tabelas já existentes.
  // Isso pode causar ALTER/DROP indesejado (ex.: tentar remover colunas).
  // Por padrão fica desligado. Para ligar explicitamente (apenas dev): TYPEORM_SYNC=true
  synchronize: process.env.TYPEORM_SYNC === 'true' ? true : false,
  logging: false,
  entities: [Users, Companies, Groups, Platforms, CompanyPlatforms, CompanyUsers, Customers, Orders, OrderItems, Products, Logs],
});
