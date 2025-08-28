"""
BBH Holdings — simple ERP-lite tracker (Streamlit + SQLite)

Features:
- Companies / Subsidiaries
- Clients
- Contracts (signed date, value, retainer, percent split, status)
- Revenue entries (payments, retainers)
- Expenses (costs to deliver on a contract)
- Equity awards tied to contracts
- Rollups: subsidiary-level and BBH (parent) top-down view
- CSV export for tables

Run:
    pip install -r requirements.txt
    streamlit run app.py

requirements.txt:
streamlit
sqlalchemy
pandas
python-dateutil
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import pandas as pd
import streamlit as st
from dateutil.parser import parse as parse_dt
from sqlalchemy import (Boolean, Column, Date, DateTime, Float, ForeignKey,
                        Integer, Numeric, String, create_engine, func)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker

# ---------- Database setup ----------
Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    is_parent = Column(Boolean, default=False)
    subsidiaries = relationship("Subsidiary", back_populates="company")


class Subsidiary(Base):
    __tablename__ = "subsidiaries"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    company = relationship("Company", back_populates="subsidiaries")
    contracts = relationship("Contract", back_populates="subsidiary")


class Client(Base):
    __tablename__ = "clients"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    contact = Column(String, nullable=True)
    email = Column(String, nullable=True)
    contracts = relationship("Contract", back_populates="client")


class Contract(Base):
    __tablename__ = "contracts"
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    subsidiary_id = Column(Integer, ForeignKey("subsidiaries.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    signed_date = Column(Date, nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    contract_value = Column(Numeric(14, 2), default=0)  # total value agreed
    retainer = Column(Numeric(14, 2), default=0)
    percent_to_subsidiary = Column(Float, default=100.0)  # percent of revenue that goes to that subsidiary
    status = Column(String, default="prospect")  # prospect / signed / active / completed / cancelled
    notes = Column(String, nullable=True)

    subsidiary = relationship("Subsidiary", back_populates="contracts")
    client = relationship("Client", back_populates="contracts")
    expenses = relationship("Expense", back_populates="contract")
    revenues = relationship("Revenue", back_populates="contract")
    equities = relationship("EquityAward", back_populates="contract")


class Expense(Base):
    __tablename__ = "expenses"
    id = Column(Integer, primary_key=True)
    contract_id = Column(Integer, ForeignKey("contracts.id"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    description = Column(String, nullable=True)
    date = Column(Date, default=date.today)

    contract = relationship("Contract", back_populates="expenses")


class Revenue(Base):
    __tablename__ = "revenues"
    id = Column(Integer, primary_key=True)
    contract_id = Column(Integer, ForeignKey("contracts.id"), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False)
    date = Column(Date, default=date.today)
    description = Column(String, nullable=True)

    contract = relationship("Contract", back_populates="revenues")


class EquityAward(Base):
    __tablename__ = "equity_awards"
    id = Column(Integer, primary_key=True)
    contract_id = Column(Integer, ForeignKey("contracts.id"), nullable=False)
    recipient = Column(String, nullable=False)  # e.g., client name / partner
    percent = Column(Float, nullable=False)  # percent ownership awarded
    notes = Column(String, nullable=True)
    date = Column(Date, default=date.today)

    contract = relationship("Contract", back_populates="equities")


engine = create_engine("sqlite:///black_bear_holdings.db", connect_args={"check_same_thread": False})
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)


# ---------- Utility functions ----------
def get_session():
    return SessionLocal()


def money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return str(x)


def calc_contract_aggregates(sess: Session, contract: Contract):
    total_revenue = float(sess.query(func.coalesce(func.sum(Revenue.amount), 0)).filter(Revenue.contract_id == contract.id).scalar() or 0)
    total_expenses = float(sess.query(func.coalesce(func.sum(Expense.amount), 0)).filter(Expense.contract_id == contract.id).scalar() or 0)
    profit = total_revenue - total_expenses
    subsidiary_share = profit * (contract.percent_to_subsidiary or 0) / 100.0
    return {
        "revenue": total_revenue,
        "expenses": total_expenses,
        "profit": profit,
        "subsidiary_share": subsidiary_share
    }


def aggregate_for_subsidiary(sess: Session, subsidiary_id: int):
    contracts = sess.query(Contract).filter(Contract.subsidiary_id == subsidiary_id).all()
    agg = {"revenue": 0.0, "expenses": 0.0, "profit": 0.0, "contracts": len(contracts)}
    for c in contracts:
        a = calc_contract_aggregates(sess, c)
        agg["revenue"] += a["revenue"]
        agg["expenses"] += a["expenses"]
        agg["profit"] += a["profit"]
    return agg


def aggregate_for_company(sess: Session, company_id: int):
    subs = sess.query(Subsidiary).filter(Subsidiary.company_id == company_id).all()
    agg = {"revenue": 0.0, "expenses": 0.0, "profit": 0.0, "subsidiaries": len(subs)}
    for s in subs:
        a = aggregate_for_subsidiary(sess, s.id)
        agg["revenue"] += a["revenue"]
        agg["expenses"] += a["expenses"]
        agg["profit"] += a["profit"]
    return agg


# ---------- Streamlit UI ----------
st.set_page_config(page_title="BBH Tracker", layout="wide")
st.title("BBH — Contracts & Financial Tracker (MVP)")

sess = get_session()

# Sidebar: quick selects + initialization
st.sidebar.header("Quick actions")

if st.sidebar.button("Create default parent & subsidiaries (BBH, NGS, 3SM)"):
    # idempotent creation
    parent = sess.query(Company).filter_by(name="BBH").first()
    if parent is None:
        parent = Company(name="Black Bear Holdings", is_parent=True)
        sess.add(parent)
        sess.commit()
    for name in ("NexxusGovSec", "3SixMedia"):
        sub = sess.query(Subsidiary).filter_by(name=name).first()
        if sub is None:
            sub = Subsidiary(name=name, company_id=parent.id)
            sess.add(sub)
    sess.commit()
    st.sidebar.success("Created BBH, NGS, and 3SM (if missing).")

companies = sess.query(Company).all()
company_map = {c.id: c.name for c in companies}
parent_company_id = companies[0].id if companies else None

st.sidebar.markdown("### Export / maintenance")
if st.sidebar.button("Export all contracts CSV"):
    contracts = sess.query(Contract).all()
    rows = []
    for c in contracts:
        a = calc_contract_aggregates(sess, c)
        rows.append({
            "contract_id": c.id,
            "title": c.title,
            "subsidiary": c.subsidiary.name,
            "client": c.client.name,
            "status": c.status,
            "contract_value": float(c.contract_value or 0),
            "retainer": float(c.retainer or 0),
            "signed_date": c.signed_date.isoformat() if c.signed_date else "",
            "revenue": a["revenue"],
            "expenses": a["expenses"],
            "profit": a["profit"],
        })
    df = pd.DataFrame(rows)
    csv = df.to_csv(index=False).encode("utf-8")
    st.sidebar.download_button("Download CSV", data=csv, file_name="contracts_export.csv", mime="text/csv")

# Main tabs
tabs = st.tabs(["Data entry", "Contracts & Drilldown", "BBH Top-down Dashboard", "Admin / Raw tables"])

# ------------------ Data entry ------------------
with tabs[0]:
    st.header("Add or manage data")

    st.subheader("Add client")
    with st.form("client_form", clear_on_submit=True):
        cname = st.text_input("Client name", "")
        contact = st.text_input("Contact (person)")
        email = st.text_input("Email")
        if st.form_submit_button("Create client"):
            if not cname:
                st.warning("Client name required")
            else:
                if sess.query(Client).filter_by(name=cname).first():
                    st.warning("Client exists")
                else:
                    sess.add(Client(name=cname, contact=contact, email=email))
                    sess.commit()
                    st.success(f"Created client: {cname}")

    st.subheader("Add subsidiary")
    companies = sess.query(Company).all()
    comp_options = {c.name: c.id for c in companies}
    with st.form("subsidiary_form", clear_on_submit=True):
        sname = st.text_input("Subsidiary name")
        parent_choice = st.selectbox("Parent company", options=list(comp_options.keys())) if comp_options else None
        if st.form_submit_button("Create subsidiary"):
            if not sname or not parent_choice:
                st.warning("Subsidiary name and parent required")
            else:
                pid = comp_options[parent_choice]
                if sess.query(Subsidiary).filter_by(name=sname).first():
                    st.warning("Subsidiary exists")
                else:
                    sess.add(Subsidiary(name=sname, company_id=pid))
                    sess.commit()
                    st.success(f"Created subsidiary {sname} under {parent_choice}")

    st.subheader("Create contract")
    subs = sess.query(Subsidiary).all()
    clients = sess.query(Client).all()
    subs_options = {s.name: s.id for s in subs}
    clients_options = {c.name: c.id for c in clients}
    with st.form("contract_form", clear_on_submit=True):
        title = st.text_input("Contract title")
        sub_choice = st.selectbox("Subsidiary", options=list(subs_options.keys())) if subs_options else None
        client_choice = st.selectbox("Client", options=list(clients_options.keys())) if clients_options else None
        contract_value = st.number_input("Contract value (total)", min_value=0.0, value=0.0, step=100.0)
        retainer = st.number_input("Retainer amount", min_value=0.0, value=0.0, step=50.0)
        percent_to_sub = st.number_input("Percent to subsidiary (share of profit)", min_value=0.0, max_value=100.0, value=100.0)
        signed_date = st.date_input("Signed date (leave if not signed)", value=None)
        status = st.selectbox("Status", options=["prospect", "signed", "active", "completed", "cancelled"], index=0)
        notes = st.text_area("Notes", "")
        if st.form_submit_button("Create contract"):
            if not title or not sub_choice or not client_choice:
                st.warning("Title, subsidiary, and client required")
            else:
                c = Contract(
                    title=title,
                    subsidiary_id=subs_options[sub_choice],
                    client_id=clients_options[client_choice],
                    contract_value=Decimal(str(contract_value)),
                    retainer=Decimal(str(retainer)),
                    percent_to_subsidiary=float(percent_to_sub),
                    signed_date=signed_date if signed_date else None,
                    status=status,
                    notes=notes
                )
                sess.add(c)
                sess.commit()
                st.success(f"Created contract {title}")

    st.subheader("Record revenue / expense / equity")
    contracts = sess.query(Contract).all()
    contract_map = {f"{c.id} - {c.title} ({c.subsidiary.name})": c.id for c in contracts}
    with st.form("finance_form", clear_on_submit=True):
        kind = st.selectbox("Record type", options=["Revenue", "Expense", "Equity Award"])
        selected = st.selectbox("Contract", options=list(contract_map.keys())) if contract_map else None
        if selected:
            cid = contract_map[selected]
            if kind == "Revenue":
                amt = st.number_input("Amount", min_value=0.0, value=0.0, step=10.0)
                desc = st.text_input("Description")
                rdate = st.date_input("Date", value=date.today())
                if st.form_submit_button("Add revenue"):
                    sess.add(Revenue(contract_id=cid, amount=Decimal(str(amt)), date=rdate, description=desc))
                    sess.commit()
                    st.success("Revenue recorded")
            elif kind == "Expense":
                amt = st.number_input("Amount", min_value=0.0, value=0.0, step=10.0)
                desc = st.text_input("Description")
                rdate = st.date_input("Date", value=date.today())
                if st.form_submit_button("Add expense"):
                    sess.add(Expense(contract_id=cid, amount=Decimal(str(amt)), date=rdate, description=desc))
                    sess.commit()
                    st.success("Expense recorded")
            else:  # Equity
                recipient = st.text_input("Recipient (who receives equity)")
                pct = st.number_input("Percent awarded", min_value=0.0, max_value=100.0, value=0.0)
                notes = st.text_area("Notes")
                edate = st.date_input("Date", value=date.today())
                if st.form_submit_button("Award equity"):
                    sess.add(EquityAward(contract_id=cid, recipient=recipient, percent=float(pct), notes=notes, date=edate))
                    sess.commit()
                    st.success("Equity award recorded")

# ------------------ Contracts & Drilldown ------------------
with tabs[1]:
    st.header("Contracts list & drilldown")
    q = sess.query(Contract).order_by(Contract.id.desc()).all()
    rows = []
    for c in q:
        a = calc_contract_aggregates(sess, c)
        rows.append({
            "ID": c.id,
            "Title": c.title,
            "Subsidiary": c.subsidiary.name,
            "Client": c.client.name,
            "Status": c.status,
            "Signed": c.signed_date.isoformat() if c.signed_date else "",
            "Contract value": float(c.contract_value or 0),
            "Revenue": a["revenue"],
            "Expenses": a["expenses"],
            "Profit": a["profit"],
        })
    df = pd.DataFrame(rows)
    st.dataframe(df)

    st.markdown("---")
    st.subheader("Contract detail")
    contract_ids = [c.id for c in q]
    if contract_ids:
        sel = st.selectbox("Choose contract ID", options=contract_ids)
        contract = sess.query(Contract).get(sel)
        st.write(f"**{contract.title}** — {contract.subsidiary.name} → {contract.client.name}")
        st.write("Status:", contract.status)
        st.write("Signed date:", contract.signed_date)
        st.write("Contract value:", money(contract.contract_value))
        st.write("Retainer:", money(contract.retainer))
        st.write("Percent to subsidiary:", f"{contract.percent_to_subsidiary}%")
        st.write("Notes:", contract.notes)

        a = calc_contract_aggregates(sess, contract)
        st.metric("Total revenue", money(a["revenue"]))
        st.metric("Total expenses", money(a["expenses"]))
        st.metric("Profit", money(a["profit"]))

        st.markdown("#### Revenues")
        revs = sess.query(Revenue).filter(Revenue.contract_id == contract.id).order_by(Revenue.date.desc()).all()
        if revs:
            rev_df = pd.DataFrame([{"date": r.date, "amount": float(r.amount), "desc": r.description} for r in revs])
            st.dataframe(rev_df)
        else:
            st.write("No revenue entries")

        st.markdown("#### Expenses")
        exps = sess.query(Expense).filter(Expense.contract_id == contract.id).order_by(Expense.date.desc()).all()
        if exps:
            exp_df = pd.DataFrame([{"date": e.date, "amount": float(e.amount), "desc": e.description} for e in exps])
            st.dataframe(exp_df)
        else:
            st.write("No expense entries")

        st.markdown("#### Equity awards")
        eqs = sess.query(EquityAward).filter(EquityAward.contract_id == contract.id).all()
        if eqs:
            eq_df = pd.DataFrame([{"recipient": e.recipient, "percent": e.percent, "date": e.date, "notes": e.notes} for e in eqs])
            st.dataframe(eq_df)
        else:
            st.write("No equity awards")

# ------------------ Top-down Dashboard ------------------
with tabs[2]:
    st.header("BBH Top-down dashboard")

    # Show companies and their aggregates
    companies = sess.query(Company).all()
    rows = []
    for c in companies:
        agg = aggregate_for_company(sess, c.id)
        rows.append({
            "company_id": c.id,
            "company": c.name,
            "subsidiaries": agg["subsidiaries"],
            "revenue": agg["revenue"],
            "expenses": agg["expenses"],
            "profit": agg["profit"],
        })
    dfc = pd.DataFrame(rows)
    st.subheader("Company rollup")
    st.dataframe(dfc)

    # Drill into BBH specifically (if exists)
    parent = sess.query(Company).filter_by(is_parent=True).first()
    if parent:
        st.markdown("---")
        st.subheader(f"BBH rollup (company: {parent.name})")
        agg = aggregate_for_company(sess, parent.id)
        st.metric("Total revenue (BBH)", money(agg["revenue"]))
        st.metric("Total expenses (BBH)", money(agg["expenses"]))
        st.metric("Total profit (BBH)", money(agg["profit"]))

        st.markdown("#### Subsidiary breakdown")
        subs = sess.query(Subsidiary).filter(Subsidiary.company_id == parent.id).all()
        rows = []
        for s in subs:
            a = aggregate_for_subsidiary(sess, s.id)
            rows.append({"subsidiary": s.name, "revenue": a["revenue"], "expenses": a["expenses"], "profit": a["profit"], "contracts": a["contracts"]})
        st.dataframe(pd.DataFrame(rows))

    # Simple time series of revenue by date across contracts
    st.markdown("---")
    st.subheader("Revenue time series (all contracts)")
    rev_q = sess.query(Revenue).order_by(Revenue.date).all()
    if rev_q:
        rev_df = pd.DataFrame([{"date": r.date, "amount": float(r.amount)} for r in rev_q])
        rev_df = rev_df.groupby("date").sum().reset_index()
        st.line_chart(rev_df.rename(columns={"date": "index"}).set_index("index")["amount"])
    else:
        st.write("No revenue records yet.")

# ------------------ Admin / Raw tables ------------------
with tabs[3]:
    st.header("Raw tables & admin")
    if st.checkbox("Show companies"):
        st.dataframe(pd.DataFrame([{"id": c.id, "name": c.name, "is_parent": c.is_parent} for c in sess.query(Company).all()]))

    if st.checkbox("Show subsidiaries"):
        st.dataframe(pd.DataFrame([{"id": s.id, "name": s.name, "company": s.company.name} for s in sess.query(Subsidiary).all()]))

    if st.checkbox("Show clients"):
        st.dataframe(pd.DataFrame([{"id": c.id, "name": c.name, "contact": c.contact, "email": c.email} for c in sess.query(Client).all()]))

    if st.checkbox("Show contracts"):
        q = sess.query(Contract).all()
        st.dataframe(pd.DataFrame([{"id": c.id, "title": c.title, "subsidiary": c.subsidiary.name, "client": c.client.name, "status": c.status} for c in q]))

    if st.checkbox("Wipe DB (delete all data)"):
        if st.button("Confirm wipe database"):
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)
            st.experimental_rerun()

