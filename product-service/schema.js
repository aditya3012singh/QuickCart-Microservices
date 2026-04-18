const { z } = require("zod");

const createProductSchema = z.object({
  name: z.string().min(1, "Name is required"),
  price: z.number().positive("Price must be positive"),
  stock: z.number().int().min(0, "Stock cannot be negative"),
});

const updateStockSchema = z.object({
  quantity: z.number().int().positive("Quantity must be positive"),
});

module.exports = {
  createProductSchema,
  updateStockSchema,
};