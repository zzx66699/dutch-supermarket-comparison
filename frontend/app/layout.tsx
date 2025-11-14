import './globals.css';
import Link from 'next/link';
import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Supermarket Compare',
  description: 'Compare supermarket prices in NL',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <header className="p-4 border-b mb-6 flex gap-4">
          <Link href="/compare" className="text-blue-600 hover:underline">
            Compare
          </Link>
          <Link href="/promotions" className="text-blue-600 hover:underline">
            Promotions
          </Link>
        </header>

        <main>{children}</main>
      </body>
    </html>
  );
}
