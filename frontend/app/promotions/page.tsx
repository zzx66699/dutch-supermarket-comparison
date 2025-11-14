'use client';

import { useEffect, useState } from 'react';
import { supabase } from '@/lib/supabaseClient';

type Product = {
  url: string | null;
  product_name_du: string | null;
  unit_du: string | null;
  current_price: number | null;
  regular_price: number | null;
  valid_from: string | null;
  valid_to: string | null;
  supermarket: string | null;
};

export default function PromotionsPage() {
  const [products, setProducts] = useState<Product[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadPromotions = async () => {
      setLoading(true);
      setError(null);

      const { data, error } = await supabase
        .from('v_promotions') // <-- use the view
        .select(
          'url, product_name_du, unit_du, current_price, regular_price, valid_from, valid_to, supermarket'
        )
        .eq('supermarket', 'dirk') // Dirk only for now
        .limit(100);

      if (error) {
        console.error(error);
        setError(error.message);
        setProducts([]);
      } else {
        setProducts(data || []);
      }

      setLoading(false);
    };

    loadPromotions();
  }, []);

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-2xl font-bold mb-4">Promotions (Dirk)</h1>

      {loading && <p>Loading...</p>}
      {error && <p className="text-red-600">Error: {error}</p>}

      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="border-b">
            <th className="text-left p-2">Product (DU)</th>
            <th className="text-left p-2">Unit (raw)</th>
            <th className="text-right p-2">Current price</th>
            <th className="text-right p-2">Regular price</th>
            <th className="text-left p-2">Valid from</th>
            <th className="text-left p-2">Valid to</th>
          </tr>
        </thead>

        <tbody>
          {products.map((p) => (
            <tr
              key={p.url || p.product_name_du || Math.random()}
              className="border-b hover:bg-gray-50"
            >
              <td className="p-2">{p.product_name_du}</td>
              <td className="p-2">{p.unit_du}</td>
              <td className="p-2 text-right">
                {p.current_price != null ? `€${p.current_price.toFixed(2)}` : '-'}
              </td>
              <td className="p-2 text-right">
                {p.regular_price != null ? `€${p.regular_price.toFixed(2)}` : '-'}
              </td>
              <td className="p-2">{p.valid_from}</td>
              <td className="p-2">{p.valid_to}</td>
            </tr>
          ))}

          {products.length === 0 && !loading && (
            <tr>
              <td colSpan={6} className="p-2 text-center text-gray-500">
                No promotions found.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
