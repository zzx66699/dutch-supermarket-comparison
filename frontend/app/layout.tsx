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

        <main>{children}</main>
      </body>
    </html>
  );
}
