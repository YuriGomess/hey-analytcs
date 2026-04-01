import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_cursor

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="app/dashboard/templates")


def safe_div(numerator: float, denominator: float) -> float:
    return (numerator / denominator) if denominator else 0.0


def money_brl(value: float | Decimal) -> str:
    num = float(value or 0)
    text = f"{num:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
    return f"R$ {text}"


def pct(value: float) -> str:
    return f"{(value or 0) * 100:.2f}%"


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    # Accept ?days=7|14|30 to control period for ALL metrics (cards, table, chart)
    raw_days = request.query_params.get("days", "30")
    try:
        selected_days = int(raw_days)
    except (ValueError, TypeError):
        selected_days = 30
    if selected_days not in (7, 14, 30):
        selected_days = 30
    interval_days = selected_days - 1  # e.g. 30 days = today minus 29

    try:
        with get_db_cursor() as (_, cur):
            cur.execute(
                """
                WITH metrics_period AS (
                    SELECT campaign_id,
                           SUM(impressions) AS impressions,
                           AVG(cpm) AS cpm,
                           SUM(clicks) AS clicks,
                           SUM(link_clicks) AS link_clicks,
                           AVG(ctr) AS ctr,
                           AVG(cpc) AS cpc,
                           SUM(page_views) AS page_views,
                           SUM(meta_forms) AS meta_forms,
                           AVG(cost_per_page_view) AS cost_per_page_view,
                           SUM(spend) AS spend
                    FROM campaign_metrics
                    WHERE date >= CURRENT_DATE - %s * INTERVAL '1 day'
                    GROUP BY campaign_id
                ),
                leads_period AS (
                    SELECT l.campaign_id,
                           COUNT(*) AS typeform_leads,
                           SUM(CASE WHEN COALESCE(l.is_mql, FALSE) THEN 1 ELSE 0 END) AS mql_count,
                           SUM(CASE WHEN COALESCE(ls.responded, FALSE) THEN 1 ELSE 0 END) AS responded,
                           SUM(CASE WHEN COALESCE(ls.meeting_scheduled, FALSE) THEN 1 ELSE 0 END) AS meeting_scheduled,
                           SUM(CASE WHEN COALESCE(ls.meeting_done, FALSE) THEN 1 ELSE 0 END) AS meeting_done,
                           SUM(CASE WHEN COALESCE(ls.sale, FALSE) THEN 1 ELSE 0 END) AS sale
                    FROM leads l
                    LEFT JOIN lead_status ls ON ls.lead_id = l.id
                    WHERE l.created_at >= NOW() - %s * INTERVAL '1 day'
                    GROUP BY l.campaign_id
                )
                SELECT c.campaign_id,
                       c.campaign_name,
                       c.lp_url,
                       c.adset_name,
                       c.ad_name,
                       COALESCE(m.impressions, 0),
                       COALESCE(m.cpm, 0),
                       COALESCE(m.clicks, 0),
                       COALESCE(m.link_clicks, 0),
                       COALESCE(m.ctr, 0),
                       COALESCE(m.cpc, 0),
                       COALESCE(m.page_views, 0),
                       COALESCE(m.meta_forms, 0),
                       COALESCE(m.cost_per_page_view, 0),
                       COALESCE(m.spend, 0),
                       COALESCE(l.typeform_leads, 0),
                       COALESCE(l.mql_count, 0),
                       COALESCE(l.responded, 0),
                       COALESCE(l.meeting_scheduled, 0),
                       COALESCE(l.meeting_done, 0),
                       COALESCE(l.sale, 0)
                FROM campaigns c
                LEFT JOIN metrics_period m ON m.campaign_id = c.campaign_id
                LEFT JOIN leads_period l ON l.campaign_id = c.campaign_id
                ORDER BY COALESCE(m.spend, 0) DESC, c.campaign_name ASC
                """,
                (interval_days, selected_days),
            )
            campaign_rows = cur.fetchall()

            cur.execute(
                """
                SELECT campaign_id, date::text, SUM(page_views) AS page_views, SUM(spend) AS spend, SUM(link_clicks) AS link_clicks
                     , SUM(meta_forms) AS meta_forms
                FROM campaign_metrics
                WHERE date >= CURRENT_DATE - %s * INTERVAL '1 day'
                GROUP BY campaign_id, date
                ORDER BY date ASC
                """,
                (interval_days,),
            )
            metric_series_rows = cur.fetchall()

            cur.execute(
                """
                SELECT l.id,
                       l.name,
                       COALESCE(l.campaign_name, c.campaign_name, l.utm_campaign, 'Sem campanha') AS campaign_name,
                       l.created_at,
                       COALESCE(ls.responded, FALSE),
                       COALESCE(ls.meeting_scheduled, FALSE),
                       COALESCE(ls.meeting_done, FALSE),
                       COALESCE(ls.sale, FALSE),
                       COALESCE(l.is_mql, FALSE),
                       l.campaign_match_status,
                       l.phone
                FROM leads l
                LEFT JOIN campaigns c ON c.campaign_id = l.campaign_id
                LEFT JOIN lead_status ls ON ls.lead_id = l.id
                ORDER BY l.created_at DESC
                LIMIT 50
                """
            )
            recent_leads_rows = cur.fetchall()

        campaign_cards = []
        totals = defaultdict(float)
        avg_ticket = 1000.0

        for row in campaign_rows:
            (
                campaign_id,
                campaign_name,
                lp_url,
                adset_name,
                ad_name,
                impressions,
                cpm,
                clicks,
                link_clicks,
                ctr,
                cpc,
                page_views,
                meta_forms,
                cost_per_page_view,
                spend,
                typeform_leads,
                mql_count,
                responded,
                meeting_scheduled,
                meeting_done,
                sale,
            ) = row

            connect_rate = safe_div(float(page_views), float(link_clicks))
            # Forms come from Meta (invitee_event_type_page conversion)
            lp_to_form = safe_div(float(meta_forms), float(page_views))
            cost_per_form = safe_div(float(spend), float(meta_forms))
            # Leads come from Typeform (matched by utm_campaign)
            lp_to_lead = safe_div(float(typeform_leads), float(page_views))
            cost_per_lead = safe_div(float(spend), float(typeform_leads))
            # MQL come from Typeform (classified by form answers)
            lp_to_mql = safe_div(float(mql_count), float(page_views))
            cost_per_mql = safe_div(float(spend), float(mql_count))
            lp_to_meeting_scheduled = safe_div(float(meeting_scheduled), float(page_views))
            lp_to_meeting_done = safe_div(float(meeting_done), float(page_views))
            lp_to_sale = safe_div(float(sale), float(page_views))
            roas = safe_div(float(sale) * avg_ticket, float(spend))
            cpl = safe_div(float(spend), float(typeform_leads)) if typeform_leads else safe_div(float(spend), float(meta_forms))

            campaign_cards.append(
                {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "lp_url": lp_url or "-",
                    "impressions": int(impressions or 0),
                    "cpm": float(cpm or 0),
                    "clicks": int(clicks or 0),
                    "link_clicks": int(link_clicks or 0),
                    "ctr": float(ctr or 0),
                    "cpc": float(cpc or 0),
                    "page_views": int(page_views or 0),
                    "cost_per_page_view": float(cost_per_page_view or 0),
                    "connect_rate": connect_rate,
                    "lp_to_form": lp_to_form,
                    "forms": int(meta_forms or 0),
                    "typeform_leads": int(typeform_leads or 0),
                    "mql": int(mql_count or 0),
                    "lp_to_lead": lp_to_lead,
                    "lp_to_mql": lp_to_mql,
                    "cost_per_lead": cost_per_lead,
                    "cost_per_mql_camp": cost_per_mql,
                    "responded": int(responded or 0),
                    "meeting_scheduled": int(meeting_scheduled or 0),
                    "meeting_done": int(meeting_done or 0),
                    "sale": int(sale or 0),
                    "cpl": cpl,
                    "cost_per_form": cost_per_form,
                    "lp_to_meeting_scheduled": lp_to_meeting_scheduled,
                    "lp_to_meeting_done": lp_to_meeting_done,
                    "lp_to_sale": lp_to_sale,
                    "roas": roas,
                    "children": [
                        {
                            "adset_name": adset_name or "-",
                            "ad_name": ad_name or "-",
                            "forms": int(meta_forms or 0),
                            "typeform_leads": int(typeform_leads or 0),
                            "mql": int(mql_count or 0),
                            "sales": int(sale or 0),
                            "spend": float(spend or 0),
                            "lp_to_form": lp_to_form,
                            "lp_to_lead": lp_to_lead,
                            "lp_to_sale": lp_to_sale,
                        }
                    ],
                }
            )

            totals["meta_forms"] += float(meta_forms or 0)
            totals["typeform_leads"] += float(typeform_leads or 0)
            totals["mql"] += float(mql_count or 0)
            totals["spend"] += float(spend or 0)
            totals["page_views"] += float(page_views or 0)
            totals["meeting_scheduled"] += float(meeting_scheduled or 0)
            totals["meeting_done"] += float(meeting_done or 0)
            totals["sale"] += float(sale or 0)
            totals["responded"] += float(responded or 0)

        # Summaries use Typeform leads as primary lead count.
        # If no Typeform leads yet, fall back to Meta forms.
        total_leads = int(totals["typeform_leads"]) if totals["typeform_leads"] else int(totals["meta_forms"])
        total_mql = int(totals["mql"])

        summaries = {
            "total_leads": total_leads,
            "total_spend": totals["spend"],
            "avg_cpl": safe_div(totals["spend"], total_leads),
            "mql": total_mql,
            "cost_per_mql": safe_div(totals["spend"], total_mql),
            "meta_forms": int(totals["meta_forms"]),
            "typeform_leads": int(totals["typeform_leads"]),
            "meetings_scheduled": int(totals["meeting_scheduled"]),
            "meetings_done": int(totals["meeting_done"]),
            "sales": int(totals["sale"]),
            "lp_to_sale": safe_div(totals["sale"], totals["page_views"]),
        }

        campaign_day = defaultdict(lambda: defaultdict(dict))
        date_set = set()
        for campaign_id, day, page_views, spend, link_clicks, meta_forms in metric_series_rows:
            campaign_day[campaign_id][day] = {
                "page_views": int(page_views or 0),
                "spend": float(spend or 0),
                "link_clicks": int(link_clicks or 0),
                "meta_forms": int(meta_forms or 0),
            }
            date_set.add(day)

        leads_by_campaign_day = defaultdict(lambda: defaultdict(lambda: {"meeting_scheduled": 0, "meeting_done": 0, "sale": 0}))
        with get_db_cursor() as (_, cur):
            cur.execute(
                """
                SELECT l.campaign_id,
                       DATE(l.created_at)::text AS day,
                       SUM(CASE WHEN COALESCE(ls.meeting_scheduled, FALSE) THEN 1 ELSE 0 END) AS meeting_scheduled,
                       SUM(CASE WHEN COALESCE(ls.meeting_done, FALSE) THEN 1 ELSE 0 END) AS meeting_done,
                       SUM(CASE WHEN COALESCE(ls.sale, FALSE) THEN 1 ELSE 0 END) AS sale
                FROM leads l
                LEFT JOIN lead_status ls ON ls.lead_id = l.id
                WHERE l.created_at >= NOW() - %s * INTERVAL '1 day'
                GROUP BY l.campaign_id, DATE(l.created_at)
                """,
                (selected_days,),
            )
            for campaign_id, day, ms, md, sale in cur.fetchall():
                leads_by_campaign_day[campaign_id][day] = {
                    "meeting_scheduled": int(ms or 0),
                    "meeting_done": int(md or 0),
                    "sale": int(sale or 0),
                }
                date_set.add(day)

        ordered_dates = sorted(date_set)
        campaign_series = []
        for campaign in campaign_cards:
            cid = campaign["campaign_id"]
            lp_to_form_series = []
            lp_to_meeting_scheduled_series = []
            lp_to_meeting_done_series = []
            lp_to_sale_series = []

            for day in ordered_dates:
                pv = campaign_day[cid].get(day, {}).get("page_views", 0)
                forms = campaign_day[cid].get(day, {}).get("meta_forms", 0)
                ms = leads_by_campaign_day[cid].get(day, {}).get("meeting_scheduled", 0)
                md = leads_by_campaign_day[cid].get(day, {}).get("meeting_done", 0)
                sale = leads_by_campaign_day[cid].get(day, {}).get("sale", 0)
                lp_to_form_series.append(round(safe_div(forms, pv) * 100, 4))
                lp_to_meeting_scheduled_series.append(round(safe_div(ms, pv) * 100, 4))
                lp_to_meeting_done_series.append(round(safe_div(md, pv) * 100, 4))
                lp_to_sale_series.append(round(safe_div(sale, pv) * 100, 4))

            campaign_series.append(
                {
                    "campaign_id": cid,
                    "campaign_name": campaign["campaign_name"],
                    "lp_to_form": lp_to_form_series,
                    "lp_to_meeting_scheduled": lp_to_meeting_scheduled_series,
                    "lp_to_meeting_done": lp_to_meeting_done_series,
                    "lp_to_sale": lp_to_sale_series,
                }
            )

        recent_leads = []
        for lead_id, name, campaign_name, created_at, responded, meeting_scheduled, meeting_done, sale, is_mql, match_status, phone in recent_leads_rows:
            status = "Novo"
            if sale:
                status = "Venda"
            elif meeting_done:
                status = "Reunião realizada"
            elif meeting_scheduled:
                status = "Reunião agendada"
            elif responded:
                status = "Respondeu"

            recent_leads.append(
                {
                    "id": lead_id,
                    "name": name or "-",
                    "phone": phone or "-",
                    "campaign_name": campaign_name or "Sem campanha",
                    "created_at": created_at.strftime("%d/%m/%Y %H:%M") if isinstance(created_at, datetime) else "-",
                    "status": status,
                    "is_mql": bool(is_mql),
                    "match_status": match_status or "-",
                }
            )

        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "campaigns": campaign_cards,
                "summaries": summaries,
                "dates": ordered_dates,
                "campaign_series": campaign_series,
                "recent_leads": recent_leads,
                "money_brl": money_brl,
                "pct": pct,
                "totals": totals,
                "selected_days": selected_days,
            },
        )
    except Exception as exc:
        logger.exception("dashboard_render_failed", extra={"error": str(exc)})
        return HTMLResponse("<h1>Erro ao carregar dashboard</h1>", status_code=500)
