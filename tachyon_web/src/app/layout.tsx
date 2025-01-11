import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Tachyon DB",
  description: "A lightweight and local time-series database",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className="antialiased"
      >
        {children}
      </body>
    </html>
  );
}
