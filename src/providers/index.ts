import type { SentinelConfig } from "../config.ts";
import { BoatProvider } from "./boat.ts";
import { CelestoProvider } from "./celesto.ts";
import { SandboxSdkProvider } from "./sandbox-sdk.ts";
import type { SandboxProvider } from "./types.ts";

export { BoatProvider } from "./boat.ts";
export { CelestoProvider, type CelestoProviderOptions } from "./celesto.ts";
export { SandboxSdkProvider, type ExternalSdkProviderLike, type SandboxSdkProviderConfig } from "./sandbox-sdk.ts";
export {
  asSandboxProvider,
  hasCapability,
  isSandboxProvider,
  type CreateSandboxOptions,
  type ExecResult,
  type ExposedEndpoint,
  type LegacyBoatClient,
  type ProviderCapability,
  type ResumeResult,
  type SandboxInfo,
  type SandboxProvider,
} from "./types.ts";

/**
 * Instantiate the configured sandbox backend from `sentinel.yaml`.
 * Defaults to native `BoatProvider` when `provider` is omitted (100% backwards-compatible).
 */
export function createSandboxProvider(config: SentinelConfig): SandboxProvider {
  const prov = config.provider;
  if (!prov || prov.type === "boat") {
    return new BoatProvider({
      bin: config.boat.bin,
      hostVisibility: config.boat.hostVisibility,
    });
  }
  if (prov.type === "sandbox-sdk") {
    return new SandboxSdkProvider({
      adapter: prov.adapter,
      options: prov.options,
      sizeMap: prov.sizeMap,
      sentinelLabel: prov.sentinelLabel,
    });
  }
  if (prov.type === "celesto") {
    return new CelestoProvider({
      apiKey: prov.apiKey,
      stateDir: config.stateDir,
      baseUrl: prov.baseUrl,
      templateId: prov.templateId,
      user: prov.user,
      sizeMap: prov.sizeMap,
    });
  }
  throw new Error(`Unsupported sandbox provider type: "${String((prov as { type?: unknown }).type)}"`);
}
