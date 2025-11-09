// frontend/src/App.jsx
import React, { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Sankey,
  CartesianGrid,
} from "recharts";

export default function App() {
  const [overview, setOverview] = useState(null);
  const [revenue, setRevenue] = useState(null);
  const [edges, setEdges] = useState([]);
  const [loading, setLoading] = useState(true);

  const BACKEND_URL = "http://127.0.0.1:8000"; // or your deployed FastAPI base URL

  useEffect(() => {
    async function loadData() {
      try {
        const [ov, rev, ed] = await Promise.all([
          fetch(`${BACKEND_URL}/analytics/overview`).then((r) => r.json()),
          fetch(`${BACKEND_URL}/analytics/revenue`).then((r) => r.json()),
          fetch(`${BACKEND_URL}/journey/edges`).then((r) => r.json()),
        ]);
        setOverview(ov);
        setRevenue(rev);
        setEdges(ed);
      } catch (err) {
        console.error("Failed to load data:", err);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  if (loading) {
    return (
      <div className="p-10 text-center text-gray-700">
        <h1 className="text-2xl font-bold mb-2">AstroArunPandit Dashboard</h1>
        <p>Loading customer journey analytics...</p>
      </div>
    );
  }

  return (
    <div className="p-6 font-sans text-gray-800">
      <h1 className="text-3xl font-bold mb-4 text-indigo-700">
        AstroArunPandit - Customer Journey Dashboard
      </h1>

      {/* Overview Stats */}
      {overview && overview.total_revenue && (
        <div className="mb-6 flex flex-col gap-1">
          <p className="text-lg">
            <strong>Total Revenue:</strong> ₹
            {overview.total_revenue.toLocaleString()}
          </p>
          <p className="text-lg">
            <strong>Total Orders:</strong> {overview.total_orders.toLocaleString()}
          </p>
        </div>
      )}

      {/* Revenue by Category */}
      {revenue?.by_category?.length > 0 && (
        <>
          <h2 className="text-xl font-semibold mb-2">Revenue by Category</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={revenue.by_category}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="category" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="amount" fill="#4f46e5" />
            </BarChart>
          </ResponsiveContainer>
        </>
      )}

      {/* Monthly Revenue */}
      {revenue?.monthly?.length > 0 && (
        <>
          <h2 className="text-xl font-semibold mt-8 mb-2">Monthly Revenue Trend</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={revenue.monthly}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="amount" fill="#16a34a" />
            </BarChart>
          </ResponsiveContainer>
        </>
      )}

      {/* Customer Journey Sankey */}
      {edges.length > 0 && (
        <>
          <h2 className="text-xl font-semibold mt-8 mb-2">
            Customer Journey Flow (Product Transitions)
          </h2>
          <ResponsiveContainer width="100%" height={500}>
            <Sankey
              data={{
                nodes: Array.from(
                  new Set(edges.flatMap((e) => [e.source, e.target]))
                ).map((name) => ({ name })),
                links: edges.map((e) => ({
                  source: e.source,
                  target: e.target,
                  value: e.value,
                })),
              }}
              nodePadding={20}
              margin={{ left: 50, right: 50, top: 20, bottom: 20 }}
            >
              <Tooltip />
            </Sankey>
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}
