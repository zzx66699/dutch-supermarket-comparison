"use client";

import { useState } from "react";

const BACKEND_URL = "https://dutch-supermarket-comparison.onrender.com"; 

export default function HomePage() {
  const [productText, setProductText] = useState("");
  const [supermarkets, setSupermarkets] = useState<string[]>([
    "ah",
    "dirk",
    "hoogvliet",
  ]);
  const [lang, setLang] = useState<"du" | "en">("du");
  const [result, setResult] = useState<string>("No results yet.");
  const [loading, setLoading] = useState(false);

  const toggleSupermarket = (value: string) => {
    setSupermarkets((prev) =>
      prev.includes(value)
        ? prev.filter((v) => v !== value)
        : [...prev, value]
    );
  };

  const performSearch = async () => {
    const queries = productText
      .split("\n")
      .map((q) => q.trim())
      .filter((q) => q.length > 0);

    if (queries.length === 0) {
      setResult("Please enter at least one product.");
      return;
    }

    const payload = {
      queries,
      search_lang: lang,
      supermarkets,
      sort_by: "unit_price",
    };

    setLoading(true);
    setResult("Searching...");

    try {
      const res = await fetch(`${BACKEND_URL}/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      setResult(JSON.stringify(data, null, 2));
    } catch (err: any) {
      setResult("Error: " + err.toString());
    } finally {
      setLoading(false);
    }
  };

  return (
    <main
      style={{
        fontFamily: "Arial, sans-serif",
        maxWidth: 700,
        margin: "40px auto",
        padding: 20,
      }}
    >
      <h1>Supermarket Product Search</h1>

      {/* Input section */}
      <section style={{ marginBottom: 20 }}>
        <label>
          <strong>Enter products (one per line):</strong>
        </label>
        <br />
        <textarea
          value={productText}
          onChange={(e) => setProductText(e.target.value)}
          placeholder={"e.g.\nvolle melk\nchicken breast"}
          style={{
            width: "100%",
            height: 100,
            padding: 10,
            fontSize: 14,
          }}
        />
      </section>

      {/* Supermarkets */}
      <section style={{ marginBottom: 20 }}>
        <strong>Select supermarkets:</strong>
        <br />
        <label>
          <input
            type="checkbox"
            checked={supermarkets.includes("ah")}
            onChange={() => toggleSupermarket("ah")}
          />{" "}
          AH
        </label>
        <br />
        <label>
          <input
            type="checkbox"
            checked={supermarkets.includes("dirk")}
            onChange={() => toggleSupermarket("dirk")}
          />{" "}
          Dirk
        </label>
        <br />
        <label>
          <input
            type="checkbox"
            checked={supermarkets.includes("hoogvliet")}
            onChange={() => toggleSupermarket("hoogvliet")}
          />{" "}
          Hoogvliet
        </label>
      </section>

      {/* Language */}
      <section style={{ marginBottom: 20 }}>
        <strong>Input language:</strong>
        <br />
        <label>
          <input
            type="radio"
            name="lang"
            value="du"
            checked={lang === "du"}
            onChange={() => setLang("du")}
          />{" "}
          Dutch (DU)
        </label>
        <label style={{ marginLeft: 10 }}>
          <input
            type="radio"
            name="lang"
            value="en"
            checked={lang === "en"}
            onChange={() => setLang("en")}
          />{" "}
          English (EN)
        </label>
      </section>

      {/* Button */}
      <button
        onClick={performSearch}
        disabled={loading}
        style={{ padding: "10px 20px", fontSize: 16 }}
      >
        {loading ? "Searching..." : "Search"}
      </button>

      {/* Results */}
      <h2>Results</h2>
      <pre
        style={{
          background: "#f4f4f4",
          padding: 15,
          borderRadius: 6,
          whiteSpace: "pre-wrap",
        }}
      >
        {result}
      </pre>
    </main>
  );
}
