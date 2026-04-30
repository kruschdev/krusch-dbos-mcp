import { Effect, Stream } from "effect";
import { HttpServerResponse } from "effect/unstable/http";

const queueStream = Stream.fromIterable(["A", "B"]).pipe(Stream.map(s => new TextEncoder().encode(s)));
try {
const response = HttpServerResponse.stream(queueStream, {
  headers: {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive"
  }
});
console.log(response);
} catch (e) { console.error(e) }
