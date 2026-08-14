"""Connector — objet du domaine (voir core/entity.py pour la règle générale).

Représente la capacité "faire parler cette organisation avec cette banque", indépendamment de
l'agrégateur réellement utilisé (Powens/Enable Banking/futur) — ce module ne connaît AUCUNE de
ces API, seulement la réalité métier ("un connector, pour un établissement+nature donnés, est
soit actif, soit en échec, soit non configuré"). Les adaptateurs (adapters/*.py) sont seuls
responsables de traduire une réponse d'API tierce en Connector ; les renderers (PrecognFlow
côté Navigator, une future vue conversationnelle...) ne consomment jamais que ceci.
"""

from .entity import Entity

FRIENDLY_NAMES = {
    "connector_powens": "Powens",
    "connector_enablebanking": "Enable Banking",
}

STATUS_ACTIVE = "active"
STATUS_ERROR = "error"
STATUS_UNCONFIGURED = "unconfigured"


class Connector(Entity):
    entity_type = "connector"

    def __init__(self, interface, etablissement, nature, status=STATUS_UNCONFIGURED, detail=None, brick_id=None):
        self.interface = interface
        self.etablissement = etablissement
        self.nature = nature
        self.status = status
        self.detail = detail or {}
        self.brick_id = brick_id

    @property
    def friendly_name(self):
        return FRIENDLY_NAMES.get(self.interface, self.interface)

    def to_dict(self):
        return {
            "entityType": self.entity_type,
            "interface": self.interface,
            "friendlyName": self.friendly_name,
            "etablissement": self.etablissement,
            "nature": self.nature,
            "status": self.status,
            "detail": self.detail,
            "brickId": self.brick_id,
        }

    @classmethod
    def unconfigured(cls, etablissement, nature):
        """Un compte sans connector résolu — pas une erreur en soi (saisie manuelle valide),
        mais une Entity Connector à part entière (jamais None/absence silencieuse) pour que les
        renderers aient toujours un objet cohérent à afficher."""
        return cls(interface=None, etablissement=etablissement, nature=nature, status=STATUS_UNCONFIGURED)
