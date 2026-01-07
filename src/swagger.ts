import swaggerJsdoc from 'swagger-jsdoc';
import swaggerUi from 'swagger-ui-express';
import { Express } from 'express';

const options: swaggerJsdoc.Options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'MOVMAIS API',
      version: '1.0.0',
      description: 'MOVMAIS API',
    },
    servers: [
      {
        url: `http://localhost:${process.env.PORT || 5003}`,
        description: 'Development server',
      },
    ],
    components: {
      schemas: {
        Users: {
          type: 'object',
          properties: {
            id: { type: 'number' },
            email: { type: 'string' },
            password: { type: 'string' },
            name: { type: 'string' },
            type: { type: 'string', enum: ['admin', 'user'] },
          },
        },
        Companies: {
          type: 'object',
          properties: {
            id: { type: 'number' },
            group_id: { type: 'number', nullable: true },
            name: { type: 'string' },
            site: { type: 'string' },
          },
        },
        Error: {
          type: 'object',
          properties: {
            message: { type: 'string' },
          },
        },
      },
    },
  },
  apis: ['./src/api/*.ts'],
};

const swaggerSpec = swaggerJsdoc(options);

export function setupSwagger(app: Express) {
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));
}
