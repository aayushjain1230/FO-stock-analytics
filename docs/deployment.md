# Deployment

## Official dashboard

The official interactive dashboard is:

```bash
streamlit run streamlit_app.py
```

`python main.py --export-report` creates an optional static report artifact. It is not the dashboard and should not be deployed through GitHub Pages.

## Streamlit Community Cloud

1. Open Streamlit Community Cloud.
2. Choose `aayushjain1230/FO-stock-analytics`.
3. Select branch `main`.
4. Set the main file path to `streamlit_app.py`.
5. Add optional secrets:

```toml
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""
SEC_USER_AGENT = ""
```

Telegram secrets are not required to launch the dashboard. `SEC_USER_AGENT` is only required for manual or background SEC synchronization.

6. Deploy.
7. Verify Home, Stocks, Opportunities, and Settings.
8. Test a watchlist refresh.
9. Test SEC disabled and enabled states.
10. Test mobile layout.

Local runtime files under `state/`, `cache/`, `plots/`, and `reports/` are not durable hosting storage and are ignored by Git.

## GitHub Actions artifacts

After a workflow run:

```text
GitHub repository -> Actions -> Select workflow run -> Artifacts
```

Artifacts may include analysis JSON, a static HTML report, SEC/model summaries, and methodology reports. Artifacts are downloadable run outputs, not permanent hosting.
