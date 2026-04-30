import { serve } from "bun";

const agentPort = 11440;

function handleStreamRequest(req: Request): Response {
  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();
      controller.enqueue(
        encoder.encode(
          `data: {"type": "info", "text": "Connected to External Agent Mock."}\n\n`,
        ),
      );

      await new Promise((r) => setTimeout(r, 500));
      controller.enqueue(
        encoder.encode(`data: {"type": "content.delta", "text": "Analyzing the task..."}\n\n`),
      );

      await new Promise((r) => setTimeout(r, 800));
      controller.enqueue(
        encoder.encode(`data: {"type": "content.delta", "text": "Executing sub-agents..."}\n\n`),
      );

      for (let i = 1; i <= 3; i++) {
        await new Promise((r) => setTimeout(r, 500));
        controller.enqueue(
          encoder.encode(`data: {"type": "content.delta", "text": "Step ${i} complete."}\n\n`),
        );
      }

      await new Promise((r) => setTimeout(r, 1000));
      controller.enqueue(
        encoder.encode(
          `data: {"type": "content.delta", "text": "Task finished successfully."}\n\n`,
        ),
      );

      controller.close();
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
}

serve({
  port: agentPort,
  fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/api/stream" && req.method === "POST") {
      return handleStreamRequest(req);
    }
    return new Response("Not Found", { status: 404 });
  },
});
console.log(`Mock External Agent Server running on port ${agentPort}`);
