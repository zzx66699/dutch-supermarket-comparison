"use client";

// Neumorphism Circle Styles
const baseCircle =
  "w-14 h-14 rounded-full flex items-center justify-center cursor-pointer text-sm font-medium select-none transition-all duration-200";

const activeRing =
  "ring-2 ring-[#4285F4] shadow-[0_0_8px_#4285F433]";


const softNeumorph =
  "bg-[#eef1f4] shadow-[2px_2px_5px_#d1d9e6,_-2px_-2px_5px_#ffffff]";

const softInset =
  "bg-[#eef1f4] shadow-[inset_3px_3px_6px_#d1d9e6,_inset_-3px_-3px_6px_#ffffff]";


const supermarketLogos = {
  ah: "logos/ah.png",
  dirk: "logos/dirk.png",
  hoogvliet: "/logos/hoogvliet.png",
};

const supermarketsList = [
  { id: "ah", label: "AH" },
  { id: "dirk", label: "Dirk" },
  { id: "hoogvliet", label: "Hoog" },
] as const;


import { useState } from "react";

const BACKEND_URL = process.env.NEXT_PUBLIC_BACKEND_URL!;

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

  // Toggle supermarket selection
  const toggleSupermarket = (value: string) => {
    setSupermarkets((prev) =>
      prev.includes(value) ? prev.filter((v) => v !== value) : [...prev, value],
    );
  };

  // Perform backend search
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
    /* main 
    - max-w-6xl: width 
    - mx-auto: Center Horizontally 
    - px-: Horizontal Padding. Sets the padding on the left and right sides. x stands for axis. 
    - pb-: Bottom Padding. 
    - pt-: Top Padding 
    - space-y-: Vertical Space Between Children. */
    <main className="max-w-6xl mx-auto px-6 pb-10 pt-20 space-y-3">

      {/* Page Title */}
      <h1 className="font-heading text-3xl md:text-5xl font-semibold text-center mb-10">
        Dutch Supermarket Price Compare
      </h1>

      {/* ===== Main Search Bar Container ===== */}
      <section className="w-full rounded-3xl backdrop-blur-xl bg-white/40 shadow-lg px-6 py-8 space-y-5">


        {/* === Top-left: Supermarkets + Language === */}
		<div className="flex flex-col gap-2">
			<p className="font-semibold">Select supermarket:</p>

          {/* Supermarkets */}
          <div className="flex gap-5">
            {supermarketsList.map((s) => {
  				const selected = supermarkets.includes(s.id);

              return (
                <div
                  key={s.id}
                  onClick={() => toggleSupermarket(s.id)}
                  className={
                    baseCircle +
                    " " +
                    (selected ? `${softInset} ${activeRing}` : softNeumorph)
                  }
                >
                  <img 
                    src={supermarketLogos[s.id]}
                    alt={s.label}
                    className={"w-10 h-10"}
				 	/>
                </div>
              );
            })}
          </div>


          {/* Language */}
          <div>
            <p className="font-semibold mb-2">Search language:</p>
            <div className="flex gap-5">
              {[
                { id: "du", label: "DU" },
                { id: "en", label: "EN" },
              ].map((lg) => {
                const selected = lang === lg.id;
                return (
                  <div
                    key={lg.id}
                    onClick={() => setLang(lg.id as "du" | "en")}
                    className={
                      baseCircle +
                      " " +
                      (selected ? `${softInset} ${activeRing}` : softNeumorph)
                    }
                  >
                    {lg.label}
                  </div>
                );
              })}
            </div>
          </div>

        </div>

        {/* === Center Search Textarea === */}
        <div>
          <label className="block mb-2 font-semibold">
            Enter products (one per line):
          </label>

          <textarea
            value={productText}
            onChange={(e) => setProductText(e.target.value)}
            placeholder={"e.g.\nvolle melk\nchicken breast"}
            className="
				w-full h-56 resize-none rounded-3xl px-6 py-5
				bg-white/30 backdrop-blur-xl
				shadow-[inset_4px_4px_8px_rgba(0,0,0,0.06),_inset_-4px_-4px_8px_rgba(255,255,255,0.5)]
				ring-1 ring-[#dbe1ea]
				focus:ring-2 focus:ring-[#4285F4]ß
				focus:ring-offset-0
				focus:border-transparent
				outline-none transition-all
				"
          />
        </div>

        {/* === Bottom-right Search Button === */}
        <div className="flex justify-end">
          <button
            onClick={performSearch}
            disabled={loading}
            className="px-8 py-3 rounded-full bg-gradient-to-br from-[#5ba0f8] to-[#2f6ce0] text-white font-medium
                        shadow-[0_4px_12px_rgba(66,133,244,0.35)]
                        hover:shadow-[0_6px_16px_rgba(66,133,244,0.45)]
                        hover:brightness-110
                        active:scale-95
                        transition-all
                        disabled:opacity-60"
          >
            {loading ? "Searching..." : "Search"}
          </button>
        </div>
      </section>

    </main>
  );
}
