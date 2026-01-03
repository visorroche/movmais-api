require('dotenv').config();

import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { Users } from './entities/Users';
import { Companies } from './entities/Companies';

export const AppDataSource = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: +(process.env.DB_PORT || 5432),
  username: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASS || 'postgres',
  database: process.env.DB_NAME || 'movmais',
  synchronize: true,
  logging: false,
  entities: [Users, Companies],
});
