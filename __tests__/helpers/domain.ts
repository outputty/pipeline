/**
 * The canonical `Order` fixture and the one VAT chain built on it (#133) - `callable.e2e.test.ts`
 * and `wrapping.e2e.test.ts` each declared this identically at their own top level, and
 * `wrapping.e2e.test.ts` shadowed it again inside two of its own `describe` blocks (moved to
 * `branch.e2e.test.ts` by this same layer). One declaration now, imported everywhere.
 */

import { Pipeline } from "@src/pipeline";

export interface Order {
  id: number;
  total: number;
  region: string;
}

export const ordersA: Order[] = [
  { id: 1, total: 50, region: "eu" },
  { id: 2, total: 300, region: "us" },
  { id: 3, total: 120, region: "eu" },
  { id: 4, total: 900, region: "us" },
];

export const ordersB: Order[] = [
  { id: 9, total: 400, region: "eu" },
  { id: 10, total: 20, region: "us" },
];

/** `ordersA` under its other name - `branch.e2e.test.ts`'s own moved cases call it `orders`; kept
 * as a separate export (not a second array) so both names resolve to the SAME reference. */
export const orders: Order[] = ordersA;

/** The canonical chain: no data, built once, reused by every case that needs a real `.transform()`
 * over `Order`s. */
export const withVat = new Pipeline<Order>().transform((t) =>
  t.map((o) => ({ ...o, total: Math.round(o.total * 1.2) })),
);
