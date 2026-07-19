import { z } from "zod";

export const scaffoldMessageSchema = z.object({
  message: z.string().min(1),
});

export type ScaffoldMessage = z.infer<typeof scaffoldMessageSchema>;
