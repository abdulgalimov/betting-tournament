import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { NestExpressApplication } from '@nestjs/platform-express';

export function setupSwagger(app: NestExpressApplication) {
  const document = SwaggerModule.createDocument(
    app,
    new DocumentBuilder()
      .setTitle('Betting Tournament API')
      .setDescription(
        'One perpetual tournament. Points are decimal USD strings. X-Player-Id is a test identity, not authentication.',
      )
      .setVersion('1.0.0')
      .build(),
  );
  SwaggerModule.setup('docs', app, document, {
    jsonDocumentUrl: 'openapi.json',
  });
}
