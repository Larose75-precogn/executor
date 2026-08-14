#!/bin/bash
# Vérifie le planning d'envoi de chaque org (fréquence/heure configurables en self-service,
# voir /api/executor/report-schedule) et envoie le rapport à celles dont c'est l'heure —
# appelé toutes les 30 min par smc-daily-report.timer (2026-07-29, retour de Stéphane : "laisser
# au user le choix de paramétrer la fréquence... et l'heure d'envoi"). Plus de liste d'orgs codée
# en dur ici : /api/executor/daily-report/check-due lit directement report_schedules.json.
curl -s -X POST http://localhost:8084/api/executor/daily-report/check-due \
  -H "Content-Type: application/json"
echo
