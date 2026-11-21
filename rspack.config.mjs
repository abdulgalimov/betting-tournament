import { resolve } from 'node:path';
import { defineConfig } from '@rspack/cli';
import nodeExternals from 'webpack-node-externals';

export default defineConfig({
  context: import.meta.dirname,
  mode: 'production',
  target: 'node',
  entry: {
    main: './src/main.ts',
    migrate: './drizzle/migrate.ts',
    'redrive-dlq': './src/cli/redrive-dlq.ts',
  },
  output: {
    path: resolve(import.meta.dirname, 'dist'),
    filename: '[name].js',
    module: true,
    clean: true,
  },
  devtool: 'source-map',
  resolve: {
    extensions: ['.ts', '.js', '.json'],
  },
  externals: [nodeExternals({ importType: 'module' })],
  module: {
    // Resolve the migration directory relative to the emitted ESM file at runtime.
    parser: { javascript: { importMeta: false, url: false } },
    rules: [
      {
        test: /\.ts$/,
        exclude: /node_modules/,
        resolve: { fullySpecified: false },
        use: {
          loader: 'builtin:swc-loader',
          options: {
            jsc: {
              target: 'es2023',
              parser: { syntax: 'typescript', decorators: true },
              transform: {
                legacyDecorator: true,
                decoratorMetadata: true,
              },
            },
          },
        },
      },
    ],
  },
  // NestJS and Pino use class/handler names at runtime.
  optimization: { minimize: false },
});
