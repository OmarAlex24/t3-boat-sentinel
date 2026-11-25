import { afterEach, describe, expect, test } from "bun:test";
import {
  HubMediaProxy,
  newMediaWsData,
  tunePreviewStreamUrl,
  type HubMediaWsData,
  type PreviewStreamTuneOptions,
} from "./hub-media.ts";

const cleanups: Array<() => void> = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
});

const until = async (cond: () => boolean, timeoutMs = 2_000) => {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("condition not met in time");
    await Bun.sleep(2);
  }
};

const asJson = (value: unknown) => JSON.stringify(value);
const parseJson = (value: string) => JSON.parse(value) as Record<string, unknown>;
const hoverMove = (x: number, y: number) => ({ type: "mouse", action: "move", button: "none", buttons: 0, x, y });
const wheel = (deltaX: number, deltaY: number, modifiers = 0, x = 100, y = 120) => ({
  type: "wheel",
  x,
  y,
  deltaX,
  deltaY,
  modifiers,
});

describe("tunePreviewStreamUrl", () => {
  test("scales a Retina-sized stream down proportionally without exposing the Hub ticket", () => {
    const tuned = tunePreviewStreamUrl(
      new URL("http://hub/api/preview-stream/ws?threadId=t1&tabId=tab1&maxWidth=2560&maxHeight=1600&wsTicket=secret"),
      { maxWidth: 1280, maxHeight: 800 },
    );
    expect(tuned.searchParams.get("maxWidth")).toBe("1280");
    expect(tuned.searchParams.get("maxHeight")).toBe("800");
    expect(tuned.searchParams.get("wsTicket")).toBeNull();
  });

  test("never upscales a client already within the caps", () => {
    const tuned = tunePreviewStreamUrl(new URL("http://hub/api/preview-stream/ws?threadId=t1&tabId=tab1&maxWidth=1000&maxHeight=800"), {
      maxWidth: 1280,
      maxHeight: 800,
    });
    expect(tuned.searchParams.get("maxWidth")).toBe("1000");
    expect(tuned.searchParams.get("maxHeight")).toBe("800");

    const partial = tunePreviewStreamUrl(new URL("http://hub/api/preview-stream/ws?threadId=t1&tabId=tab1&maxWidth=1000"), {
      maxWidth: 1280,
      maxHeight: 800,
    });
    expect(partial.searchParams.get("maxWidth")).toBe("1000");
    expect(partial.searchParams.get("maxHeight")).toBe("800");
  });

  test("sets configured dimensions when the client omitted them", () => {
    const tuned = tunePreviewStreamUrl(new URL("http://hub/api/preview-stream/ws?threadId=t1&tabId=tab1"), {
      maxWidth: 1280,
      maxHeight: 800,
    });
    expect(tuned.searchParams.get("maxWidth")).toBe("1280");
    expect(tuned.searchParams.get("maxHeight")).toBe("800");
  });

  test("injects quality, caps the client quality, and clamps the configured quality", () => {
    const base = "http://hub/api/preview-stream/ws?threadId=t1&tabId=tab1";
    expect(tunePreviewStreamUrl(new URL(base), { quality: 65 }).searchParams.get("quality")).toBe("65");
    expect(tunePreviewStreamUrl(new URL(`${base}&quality=90`), { quality: 65 }).searchParams.get("quality")).toBe("65");
    expect(tunePreviewStreamUrl(new URL(`${base}&quality=40`), { quality: 65 }).searchParams.get("quality")).toBe("40");
    expect(tunePreviewStreamUrl(new URL(base), { quality: 0 }).searchParams.get("quality")).toBe("1");
    expect(tunePreviewStreamUrl(new URL(base), { quality: 150 }).searchParams.get("quality")).toBe("100");
  });

  test("does not tune device-hub sockets", () => {
    const tuned = tunePreviewStreamUrl(new URL("http://hub/api/device-hub/ws?device=d1&maxWidth=2560&quality=90&wsTicket=secret"), {
      maxWidth: 1280,
      maxHeight: 800,
      quality: 40,
    });
    expect(tuned.searchParams.get("maxWidth")).toBe("2560");
    expect(tuned.searchParams.get("quality")).toBe("90");
    expect(tuned.searchParams.get("wsTicket")).toBeNull();
  });

  test("newMediaWsData stays backwards compatible and stores coalescer state only when requested", () => {
    const url = new URL("http://hub/api/preview-stream/ws?threadId=t1&tabId=tab1&maxWidth=2560&maxHeight=1600&wsTicket=secret");
    const plain = newMediaWsData("client", url);
    expect(plain.inputCoalescer).toBeNull();
    expect(plain.upstreamPathAndQuery).toBe("/api/preview-stream/ws?threadId=t1&tabId=tab1&maxWidth=2560&maxHeight=1600");

    const tuned = newMediaWsData("client", url, { maxWidth: 1280, maxHeight: 800, quality: 55, coalesceInput: true });
    expect(tuned.inputCoalescer).not.toBeNull();
    const upstream = new URL(`http://vm${tuned.upstreamPathAndQuery}`);
    expect(upstream.searchParams.get("maxWidth")).toBe("1280");
    expect(upstream.searchParams.get("maxHeight")).toBe("800");
    expect(upstream.searchParams.get("quality")).toBe("55");
  });
});

describe("HubMediaProxy input coalescing", () => {
  const tune: PreviewStreamTuneOptions = { coalesceInput: true, mouseMoveIntervalMs: 100, wheelIntervalMs: 90 };

  async function mediaRig(opts: { path?: string; tune?: PreviewStreamTuneOptions } = {}) {
    const vmReceived: string[] = [];
    let vmSocket: Bun.ServerWebSocket<unknown> | null = null;
    const vm = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, srv) {
        return srv.upgrade(req) ? undefined : new Response("no", { status: 400 });
      },
      websocket: {
        open(ws) {
          vmSocket = ws;
        },
        message(_ws, raw) {
          vmReceived.push(typeof raw === "string" ? raw : new TextDecoder().decode(raw));
        },
      },
    });
    cleanups.push(() => vm.stop(true));

    const proxy = new HubMediaProxy(async () => ({
      ok: true,
      sandbox: { sandboxId: "sb-input", upstreamUrl: `http://127.0.0.1:${vm.port}`, upstreamToken: "vm-token" },
    }));
    const hub = Bun.serve<HubMediaWsData, never>({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, srv) {
        const data = newMediaWsData("client-1", new URL(req.url), opts.tune);
        return srv.upgrade(req, { data }) ? undefined : new Response("no", { status: 400 });
      },
      websocket: {
        open: (ws) => proxy.open(ws),
        message: (ws, msg) => proxy.message(ws, msg),
        close: (ws, code, reason) => proxy.close(ws, code, reason),
      },
    });
    cleanups.push(() => hub.stop(true));

    const path = opts.path ?? "/api/preview-stream/ws?threadId=t1&tabId=tab1&maxWidth=800&maxHeight=600&wsTicket=hub-ticket";
    const client = new WebSocket(`ws://127.0.0.1:${hub.port}${path}`);
    cleanups.push(() => {
      try {
        client.close();
      } catch {
        // already closed
      }
    });
    await new Promise<void>((resolve, reject) => {
      client.addEventListener("open", () => resolve());
      client.addEventListener("error", () => reject(new Error("client websocket failed")));
    });
    await until(() => vmSocket !== null && proxy.count("sb-input") === 1);
    await Bun.sleep(10);
    return { client, proxy, vmReceived };
  }

  test("sends a leading hover move immediately and collapses rapid moves to the latest trailing move", async () => {
    const rig = await mediaRig({ tune });
    rig.client.send(asJson(hoverMove(10, 10)));
    await until(() => rig.vmReceived.length === 1);
    expect(parseJson(rig.vmReceived[0]).x).toBe(10);

    rig.client.send(asJson(hoverMove(11, 11)));
    rig.client.send(asJson(hoverMove(12, 12)));
    rig.client.send(asJson(hoverMove(13, 13)));
    await until(() => rig.vmReceived.length === 2);
    const trailing = parseJson(rig.vmReceived[1]);
    expect(trailing.x).toBe(13);
    expect(trailing.y).toBe(13);
    await Bun.sleep(30);
    expect(rig.vmReceived).toHaveLength(2);
  });

  test("flushes a pending hover move before a mouse click or key input", async () => {
    const rig = await mediaRig({ tune });
    rig.client.send(asJson(hoverMove(10, 10)));
    await until(() => rig.vmReceived.length === 1);

    rig.client.send(asJson(hoverMove(20, 20)));
    const mouseDown = { type: "mouse", action: "down", button: "left", buttons: 1, x: 20, y: 20 };
    rig.client.send(asJson(mouseDown));
    await until(() => rig.vmReceived.length === 3);
    expect(parseJson(rig.vmReceived[1]).x).toBe(20);
    expect(parseJson(rig.vmReceived[2])).toMatchObject({ type: "mouse", action: "down", button: "left" });

    rig.client.send(asJson(hoverMove(30, 30)));
    const key = { type: "key", action: "down", key: "a", code: "KeyA" };
    rig.client.send(asJson(key));
    await until(() => rig.vmReceived.length === 5);
    expect(parseJson(rig.vmReceived[3]).x).toBe(30);
    expect(parseJson(rig.vmReceived[4])).toMatchObject({ type: "key", key: "a" });
  });

  test("accumulates rapid wheel deltas into one trailing event using the latest position", async () => {
    const rig = await mediaRig({ tune });
    rig.client.send(asJson(wheel(1, 10, 0, 100, 120)));
    await until(() => rig.vmReceived.length === 1);

    rig.client.send(asJson(wheel(2, 20, 0, 101, 121)));
    rig.client.send(asJson(wheel(3, 30, 0, 102, 122)));
    await until(() => rig.vmReceived.length === 2);
    expect(parseJson(rig.vmReceived[1])).toMatchObject({ type: "wheel", x: 102, y: 122, deltaX: 5, deltaY: 50, modifiers: 0 });
    await Bun.sleep(30);
    expect(rig.vmReceived).toHaveLength(2);
  });

  test("flushes a pending wheel before accumulating one with different modifiers", async () => {
    const rig = await mediaRig({ tune });
    rig.client.send(asJson(wheel(1, 10, 0)));
    await until(() => rig.vmReceived.length === 1);

    rig.client.send(asJson(wheel(2, 20, 0)));
    rig.client.send(asJson(wheel(4, 40, 2)));
    await until(() => rig.vmReceived.length === 2);
    expect(parseJson(rig.vmReceived[1])).toMatchObject({ deltaX: 2, deltaY: 20, modifiers: 0 });
    await until(() => rig.vmReceived.length === 3);
    expect(parseJson(rig.vmReceived[2])).toMatchObject({ deltaX: 4, deltaY: 40, modifiers: 2 });
  });

  test("closing the client socket clears trailing timers without sending after close", async () => {
    const rig = await mediaRig({ tune });
    rig.client.send(asJson(hoverMove(10, 10)));
    await until(() => rig.vmReceived.length === 1);
    rig.client.send(asJson(hoverMove(20, 20)));
    rig.client.close();
    await until(() => rig.proxy.count("sb-input") === 0);
    await Bun.sleep(130);
    expect(rig.vmReceived).toHaveLength(1);
  });

  test("preview sockets without coalesceInput and device-hub sockets stay pass-through", async () => {
    const plain = await mediaRig();
    for (const move of [hoverMove(1, 1), hoverMove(2, 2), hoverMove(3, 3)]) plain.client.send(asJson(move));
    await until(() => plain.vmReceived.length === 3);
    plain.client.close();

    const device = await mediaRig({ path: "/api/device-hub/ws?device=d1", tune });
    for (const move of [hoverMove(4, 4), hoverMove(5, 5), wheel(1, 2)]) device.client.send(asJson(move));
    await until(() => device.vmReceived.length === 3);
    expect(parseJson(device.vmReceived[2])).toMatchObject({ type: "wheel", deltaX: 1, deltaY: 2 });
    device.client.close();
  });
});
