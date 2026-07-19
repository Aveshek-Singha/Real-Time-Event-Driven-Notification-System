import { scaffoldMessageSchema } from "@notification-system/contracts";

export default function Home() {
  const scaffoldMessage = scaffoldMessageSchema.parse({
    message: "Web app scaffold is ready.",
  });

  return (
    <main>
      <h1>Notification Center</h1>
      <p>{scaffoldMessage.message}</p>
    </main>
  );
}
