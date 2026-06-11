import { NavbarBrand, Navbar } from "react-bootstrap";
import Link from "next/link";
import React from "react";

export default function Navigation() {
  return (
    <Navbar
      expand="lg"
      className="py-3 px-4 shadow-sm"
      style={{ backgroundColor: "#ffffff" }}
    >
      <div className="container-fluid">
        {/* ✅ 로고 + 텍스트를 한 줄로 정렬 */}
        <Link
          href="/"
          passHref
          className="d-flex align-items-center text-decoration-none"
        >
          <NavbarBrand
            className="d-flex align-items-center me-0"
            style={{
              paddingLeft: "12px",
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
            }}
          >
            <img
              src="https://www.sanctionlab.com/wp-content/uploads/2024/10/s-logo.png"
              width="40"
              height="40"
              className="align-top"
              alt="SanctionLab"
            />
            <span
              style={{
                fontWeight: 700,
                fontSize: "1.3rem",
                color: "#000",
                marginLeft: "10px",
              }}
            >
              SanctionLab
            </span>
          </NavbarBrand>
        </Link>

        {/* 네비게이션 링크 */}
        <div className="d-flex align-items-center gap-3 ms-4">
          <Link
            href="/crawler"
            className="text-decoration-none text-sm font-medium"
            style={{ color: "#4b5563", fontSize: "0.9rem" }}
          >
            금융 크롤러
          </Link>
        </div>
      </div>
    </Navbar>
  );
}
