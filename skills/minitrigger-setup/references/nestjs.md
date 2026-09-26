# NestJS

Create `src/jobs/minitrigger.module.ts`:

```ts
import {
	type DynamicModule,
	Global,
	Inject,
	Injectable,
	Module,
	type OnApplicationBootstrap,
	type OnApplicationShutdown,
} from "@nestjs/common";
import { createEngine, type Engine } from "@rivia-labs/minitrigger";

export const MINITRIGGER_ENGINE = Symbol("MINITRIGGER_ENGINE");

@Injectable()
class EngineLifecycle implements OnApplicationBootstrap, OnApplicationShutdown {
	constructor(@Inject(MINITRIGGER_ENGINE) private readonly engine: Engine) {}

	async onApplicationBootstrap() {
		await this.engine.migrate();
		if (process.env.WORKER_ROLE !== "web") {
			await this.engine.start();
		}
	}

	onApplicationShutdown() {
		return this.engine.stop();
	}
}

@Global()
@Module({})
export class MinitriggerModule {
	static forRootAsync(options: {
		imports?: DynamicModule["imports"];
		inject?: any[];
		useFactory: (...deps: any[]) => Parameters<typeof createEngine>[0] | Promise<Parameters<typeof createEngine>[0]>;
	}): DynamicModule {
		return {
			module: MinitriggerModule,
			imports: options.imports ?? [],
			providers: [
				{
					provide: MINITRIGGER_ENGINE,
					inject: options.inject ?? [],
					useFactory: async (...deps: unknown[]) => createEngine(await options.useFactory(...deps)),
				},
				EngineLifecycle,
			],
			exports: [MINITRIGGER_ENGINE],
		};
	}
}
```

Register it, e.g. in `app.module.ts`:

```ts
MinitriggerModule.forRootAsync({
	useFactory: () => ({
		postgres: process.env.DATABASE_URL!,
		redis: process.env.REDIS_URL!,
		schema: "myapp",
		prefix: "myapp",
		tasks,
	}),
}),
```

`main.ts` **must** call `app.enableShutdownHooks();` — without it, Nest never invokes
`OnApplicationShutdown`, so `engine.stop()` is never called and the graceful hand-back on shutdown
never runs.

Services get the engine with `@Inject(MINITRIGGER_ENGINE) private readonly engine: Engine`. To
`trigger()` inside a transaction managed by TypeORM/Prisma/Knex, pass the underlying node-postgres
client as `{ tx }` (see the `minitrigger-outbox` skill) — Nest's own transaction managers don't
give you that client directly for every ORM, so you may need a small adapter that exposes the
active `PoolClient`.

For a web + worker split (recommended for production beyond a single instance), gate `start()`
behind an env var (`WORKER_ROLE`/`OBAN_ROLE`-style) as above, and run a second Nest application (or
a plain script importing the same `tasks` registry) with that var set for the worker process.
