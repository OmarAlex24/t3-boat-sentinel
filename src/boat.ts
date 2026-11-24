/**
 * Backwards-compatible re-exports of the Boat provider and sandbox types.
 * New code should import from `./providers/index.ts`.
 */
export {
  BoatProvider as BoatCli,
  BoatProvider,
  type BoatProviderOptions,
} from "./providers/boat.ts";
export {
  asSandboxProvider,
  hasCapability,
  type CreateSandboxOptions,
  type ExecResult as BoatExecResult,
  type ExposedEndpoint,
  type LegacyBoatClient as BoatClient,
  type ProviderCapability,
  type ResumeResult,
  type SandboxInfo as BoatSandbox,
  type SandboxProvider,
} from "./providers/types.ts";
