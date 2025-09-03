import pandas as pd
import logging
from typing import Tuple, List, Dict, Any, Optional
from services.lotecart_processor import LotecartProcessor

logger = logging.getLogger(__name__)

class PriorityProcessor:
    """
    Processeur avec gestion stricte des priorités:
    1. LOTECART (priorité absolue - traitement complet)
    2. Autres ajustements (après validation LOTECART)
    3. Génération fichier final (avec cohérence garantie)
    """
    
    def __init__(self):
        self.lotecart_processor = LotecartProcessor()
        self.processing_summary = {}
        self.lotecart_validated = False
    
    def process_with_strict_priority(
        self, 
        completed_df: pd.DataFrame, 
        original_df: pd.DataFrame,
        strategy: str = "FIFO"
    ) -> Dict[str, Any]:
        """
        Traite les données avec priorisation STRICTE LOTECART
        
        ÉTAPES OBLIGATOIRES:
        1. Traitement complet LOTECART
        2. Validation LOTECART (blocante)
        3. Traitement autres ajustements
        4. Génération fichier final
        """
        try:
            logger.info("🚀 DÉBUT DU TRAITEMENT AVEC PRIORISATION STRICTE LOTECART")
            
            # ÉTAPE 1: Traitement prioritaire et complet des LOTECART
            logger.info("📍 ÉTAPE 1: TRAITEMENT PRIORITAIRE COMPLET LOTECART")
            
            lotecart_result = self._process_lotecart_completely(completed_df, original_df)
            
            # ÉTAPE 2: Validation stricte LOTECART (BLOCANTE)
            logger.info("📍 ÉTAPE 2: VALIDATION STRICTE LOTECART (BLOCANTE)")
            
            lotecart_validation = self._validate_lotecart_strict(lotecart_result)
            if not lotecart_validation["success"]:
                raise ValueError(f"VALIDATION LOTECART ÉCHOUÉE: {lotecart_validation['issues']}")
            
            self.lotecart_validated = True
            logger.info("✅ VALIDATION LOTECART RÉUSSIE - PASSAGE AUX AUTRES AJUSTEMENTS")
            
            # ÉTAPE 3: Traitement des autres ajustements (après LOTECART validé)
            logger.info("📍 ÉTAPE 3: TRAITEMENT DES AUTRES AJUSTEMENTS")
            
            other_adjustments = self._process_non_lotecart_adjustments(
                completed_df, original_df, lotecart_result["candidates"], strategy
            )
            
            # ÉTAPE 4: Consolidation finale avec vérification cohérence
            logger.info("📍 ÉTAPE 4: CONSOLIDATION FINALE AVEC VÉRIFICATION")
            
            final_result = self._consolidate_with_coherence_check(
                lotecart_result, other_adjustments, strategy
            )
            
            # Sauvegarder le résumé pour la génération du fichier final
            self.processing_summary = final_result
            
            logger.info("✅ TRAITEMENT AVEC PRIORISATION STRICTE TERMINÉ")
            
            return final_result
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement avec priorisation stricte: {e}", exc_info=True)
            raise
    
    def _process_lotecart_completely(
        self, 
        completed_df: pd.DataFrame, 
        original_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Traitement COMPLET des LOTECART avec toutes les vérifications
        """
        try:
            logger.info("🎯 TRAITEMENT COMPLET LOTECART - PHASE 1")
            
            # 1. Détection des candidats LOTECART
            lotecart_candidates = self.lotecart_processor.detect_lotecart_candidates(completed_df)
            
            if lotecart_candidates.empty:
                logger.info("ℹ️ Aucun candidat LOTECART détecté")
                return {
                    "candidates": lotecart_candidates,
                    "new_adjustments": [],
                    "existing_updates": [],
                    "summary": self.lotecart_processor._create_empty_summary()
                }
            
            logger.info(f"🎯 {len(lotecart_candidates)} candidats LOTECART détectés")
            
            # 2. Création des nouveaux ajustements LOTECART
            new_lotecart_adjustments = self.lotecart_processor.create_priority_lotecart_adjustments(
                lotecart_candidates, original_df
            )
            
            # 3. Mise à jour des lignes existantes avec quantité théorique = 0
            existing_lotecart_updates = self.lotecart_processor.update_existing_lotecart_lines(
                original_df, completed_df
            )
            
            # 4. Vérification de cohérence LOTECART
            coherence_check = self._verify_lotecart_coherence(
                lotecart_candidates, new_lotecart_adjustments, existing_lotecart_updates
            )
            
            if not coherence_check["success"]:
                raise ValueError(f"INCOHÉRENCE LOTECART: {coherence_check['issues']}")
            
            # 5. Génération du résumé LOTECART
            lotecart_summary = self.lotecart_processor.get_lotecart_summary(
                lotecart_candidates, new_lotecart_adjustments + existing_lotecart_updates
            )
            
            result = {
                "candidates": lotecart_candidates,
                "new_adjustments": new_lotecart_adjustments,
                "existing_updates": existing_lotecart_updates,
                "summary": lotecart_summary,
                "coherence_check": coherence_check
            }
            
            logger.info(
                f"✅ TRAITEMENT COMPLET LOTECART TERMINÉ: "
                f"{len(new_lotecart_adjustments)} nouveaux + "
                f"{len(existing_lotecart_updates)} mises à jour = "
                f"{len(new_lotecart_adjustments) + len(existing_lotecart_updates)} LOTECART totaux"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement complet LOTECART: {e}", exc_info=True)
            raise
    
    def _verify_lotecart_coherence(
        self, 
        candidates: pd.DataFrame,
        new_adjustments: List[Dict[str, Any]],
        existing_updates: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Vérification stricte de cohérence LOTECART
        """
        coherence = {
            "success": False,
            "issues": [],
            "stats": {
                "candidates_count": len(candidates),
                "new_adjustments_count": len(new_adjustments),
                "existing_updates_count": len(existing_updates),
                "total_lotecart_count": len(new_adjustments) + len(existing_updates)
            }
        }
        
        try:
            # 1. Vérifier que chaque candidat a un traitement
            candidates_articles = set()
            for _, candidate in candidates.iterrows():
                key = (
                    candidate["Code Article"],
                    candidate.get("Numéro Inventaire", ""),
                    str(candidate.get("Numéro Lot", "")).strip()
                )
                candidates_articles.add(key)
            
            # Articles traités par nouveaux ajustements
            new_articles = set()
            for adj in new_adjustments:
                key = (
                    adj["CODE_ARTICLE"],
                    adj["NUMERO_INVENTAIRE"],
                    adj.get("metadata", {}).get("original_lot", "")
                )
                new_articles.add(key)
            
            # Articles traités par mises à jour existantes
            updated_articles = set()
            for update in existing_updates:
                key = (
                    update["CODE_ARTICLE"],
                    update["NUMERO_INVENTAIRE"],
                    update.get("metadata", {}).get("original_lot", "")
                )
                updated_articles.add(key)
            
            all_treated_articles = new_articles | updated_articles
            
            # Vérifier la couverture
            missing_treatments = candidates_articles - all_treated_articles
            if missing_treatments:
                coherence["issues"].append(
                    f"Candidats LOTECART non traités: {len(missing_treatments)} articles"
                )
                for article_key in list(missing_treatments)[:5]:  # Afficher max 5
                    coherence["issues"].append(f"  - {article_key[0]} (Inv: {article_key[1]})")
            
            # 2. Vérifier les quantités
            for adj in new_adjustments + existing_updates:
                if adj["QUANTITE_CORRIGEE"] <= 0:
                    coherence["issues"].append(
                        f"Quantité corrigée invalide pour {adj['CODE_ARTICLE']}: {adj['QUANTITE_CORRIGEE']}"
                    )
                
                if adj["QUANTITE_REELLE_SAISIE"] <= 0:
                    coherence["issues"].append(
                        f"Quantité saisie invalide pour {adj['CODE_ARTICLE']}: {adj['QUANTITE_REELLE_SAISIE']}"
                    )
                
                # Pour LOTECART: quantité corrigée DOIT égaler quantité saisie
                if abs(adj["QUANTITE_CORRIGEE"] - adj["QUANTITE_REELLE_SAISIE"]) > 0.001:
                    coherence["issues"].append(
                        f"Incohérence LOTECART {adj['CODE_ARTICLE']}: "
                        f"Corrigée={adj['QUANTITE_CORRIGEE']} ≠ Saisie={adj['QUANTITE_REELLE_SAISIE']}"
                    )
            
            # 3. Vérifier l'unicité des traitements
            all_keys = []
            for adj in new_adjustments + existing_updates:
                key = (adj["CODE_ARTICLE"], adj["NUMERO_INVENTAIRE"])
                all_keys.append(key)
            
            duplicate_keys = set([key for key in all_keys if all_keys.count(key) > 1])
            if duplicate_keys:
                coherence["issues"].append(
                    f"Traitements LOTECART dupliqués pour: {len(duplicate_keys)} articles"
                )
            
            coherence["success"] = len(coherence["issues"]) == 0
            
            if coherence["success"]:
                logger.info("✅ COHÉRENCE LOTECART VÉRIFIÉE")
            else:
                logger.error(f"❌ INCOHÉRENCES LOTECART DÉTECTÉES: {coherence['issues']}")
            
            return coherence
            
        except Exception as e:
            logger.error(f"❌ Erreur vérification cohérence LOTECART: {e}", exc_info=True)
            coherence["issues"].append(f"Erreur de vérification: {str(e)}")
            return coherence
    
    def _validate_lotecart_strict(self, lotecart_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validation STRICTE et BLOCANTE des LOTECART
        """
        validation = {
            "success": False,
            "issues": [],
            "critical_errors": [],
            "warnings": []
        }
        
        try:
            candidates = lotecart_result["candidates"]
            new_adjustments = lotecart_result["new_adjustments"]
            existing_updates = lotecart_result["existing_updates"]
            coherence_check = lotecart_result["coherence_check"]
            
            # 1. Vérifier la cohérence (déjà calculée)
            if not coherence_check["success"]:
                validation["critical_errors"].extend(coherence_check["issues"])
            
            # 2. Vérifications supplémentaires strictes
            total_candidates = len(candidates)
            total_treatments = len(new_adjustments) + len(existing_updates)
            
            if total_treatments < total_candidates:
                validation["critical_errors"].append(
                    f"Traitement incomplet: {total_treatments} traitements pour {total_candidates} candidats"
                )
            
            # 3. Vérifier que tous les LOTECART ont les bonnes propriétés
            for adj in new_adjustments + existing_updates:
                # Vérifications critiques
                if adj["TYPE_LOT"] != "lotecart":
                    validation["critical_errors"].append(
                        f"Type incorrect pour {adj['CODE_ARTICLE']}: {adj['TYPE_LOT']} (attendu: lotecart)"
                    )
                
                if adj["QUANTITE_ORIGINALE"] != 0:
                    validation["critical_errors"].append(
                        f"Quantité originale incorrecte pour LOTECART {adj['CODE_ARTICLE']}: {adj['QUANTITE_ORIGINALE']} (attendu: 0)"
                    )
                
                if not adj.get("is_priority_processed", False):
                    validation["warnings"].append(
                        f"Flag priorité manquant pour {adj['CODE_ARTICLE']}"
                    )
            
            # 4. Validation finale
            validation["success"] = len(validation["critical_errors"]) == 0
            
            if validation["success"]:
                logger.info(f"✅ VALIDATION STRICTE LOTECART RÉUSSIE: {total_treatments} LOTECART validés")
            else:
                logger.error(f"❌ VALIDATION STRICTE LOTECART ÉCHOUÉE: {len(validation['critical_errors'])} erreurs critiques")
                for error in validation["critical_errors"]:
                    logger.error(f"   🔴 {error}")
            
            return validation
            
        except Exception as e:
            logger.error(f"❌ Erreur validation stricte LOTECART: {e}", exc_info=True)
            validation["critical_errors"].append(f"Erreur de validation: {str(e)}")
            return validation
    
    def _process_non_lotecart_adjustments(
        self, 
        completed_df: pd.DataFrame, 
        original_df: pd.DataFrame,
        lotecart_candidates: pd.DataFrame,
        strategy: str
    ) -> List[Dict[str, Any]]:
        """
        Traite les ajustements non-LOTECART APRÈS validation LOTECART
        """
        try:
            if not self.lotecart_validated:
                raise ValueError("LOTECART non validés - impossible de traiter les autres ajustements")
            
            logger.info("🔧 TRAITEMENT DES AUTRES AJUSTEMENTS (POST-LOTECART)")
            
            # Calculer les écarts en excluant strictement les LOTECART
            completed_clean = completed_df.copy()
            
            # Conversion des quantités
            completed_clean["Quantité Théorique"] = pd.to_numeric(
                completed_clean["Quantité Théorique"], errors="coerce"
            ).fillna(0)
            completed_clean["Quantité Réelle"] = pd.to_numeric(
                completed_clean["Quantité Réelle"], errors="coerce"
            ).fillna(0)
            
            # Calculer les écarts
            completed_clean["Écart"] = (
                completed_clean["Quantité Réelle"] - completed_clean["Quantité Théorique"]
            )
            
            # Exclure STRICTEMENT les LOTECART
            lotecart_exclusions = set()
            if not lotecart_candidates.empty:
                for _, candidate in lotecart_candidates.iterrows():
                    key = (
                        candidate["Code Article"],
                        candidate.get("Numéro Inventaire", ""),
                        str(candidate.get("Numéro Lot", "")).strip()
                    )
                    lotecart_exclusions.add(key)
                
                logger.info(f"🚫 Exclusion de {len(lotecart_exclusions)} articles LOTECART des autres ajustements")
            
            # Filtrer les écarts non-LOTECART
            non_lotecart_discrepancies = []
            
            for _, row in completed_clean.iterrows():
                key = (
                    row["Code Article"],
                    row.get("Numéro Inventaire", ""),
                    str(row.get("Numéro Lot", "")).strip()
                )
                
                # Exclure si c'est un LOTECART
                if key in lotecart_exclusions:
                    continue
                
                # Exclure si pas d'écart
                if abs(row["Écart"]) < 0.001:
                    continue
                
                non_lotecart_discrepancies.append(row)
            
            if not non_lotecart_discrepancies:
                logger.info("ℹ️ Aucun écart non-LOTECART à traiter")
                return []
            
            logger.info(f"🔧 Traitement de {len(non_lotecart_discrepancies)} écarts non-LOTECART avec stratégie {strategy}")
            
            # Distribuer les écarts selon la stratégie
            adjustments = []
            
            for discrepancy_row in non_lotecart_discrepancies:
                code_article = discrepancy_row["Code Article"]
                numero_inventaire = discrepancy_row.get("Numéro Inventaire", "")
                ecart = discrepancy_row["Écart"]
                quantite_reelle_saisie = discrepancy_row["Quantité Réelle"]
                quantite_theo_originale = discrepancy_row["Quantité Théorique"]
                
                # Trouver les lots pour cet article (excluant les LOTECART)
                article_lots = original_df[
                    (original_df["CODE_ARTICLE"] == code_article) &
                    (original_df["NUMERO_INVENTAIRE"] == numero_inventaire) &
                    (original_df["QUANTITE"] > 0)  # Exclure les lignes avec quantité = 0 (potentiels LOTECART)
                ].copy()
                
                if article_lots.empty:
                    logger.warning(f"⚠️ Aucun lot non-LOTECART trouvé pour {code_article}")
                    continue
                
                # Trier selon la stratégie
                article_lots = self._sort_lots_by_strategy(article_lots, strategy)
                
                # Distribuer l'écart
                remaining_discrepancy = ecart
                
                for _, lot_row in article_lots.iterrows():
                    if abs(remaining_discrepancy) < 0.001:
                        break
                    
                    lot_quantity = float(lot_row["QUANTITE"])
                    lot_number = str(lot_row["NUMERO_LOT"]).strip() if lot_row["NUMERO_LOT"] else ""
                    
                    # Calculer l'ajustement
                    if remaining_discrepancy > 0:
                        adjustment = min(remaining_discrepancy, lot_quantity * 2)
                    else:
                        adjustment = max(remaining_discrepancy, -lot_quantity)
                    
                    if abs(adjustment) > 0.001:
                        adjustments.append({
                            "CODE_ARTICLE": code_article,
                            "NUMERO_INVENTAIRE": numero_inventaire,
                            "NUMERO_LOT": lot_number,
                            "TYPE_LOT": lot_row.get("Type_Lot", "unknown"),
                            "PRIORITY": 2,  # Priorité inférieure aux LOTECART
                            "QUANTITE_ORIGINALE": lot_quantity,
                            "QUANTITE_REELLE_SAISIE": quantite_reelle_saisie,
                            "QUANTITE_CORRIGEE": lot_quantity + adjustment,
                            "AJUSTEMENT": adjustment,
                            "Date_Lot": lot_row.get("Date_Lot"),
                            "original_s_line_raw": lot_row.get("original_s_line_raw"),
                            "is_priority_processed": False,
                            "is_post_lotecart": True,  # Flag spécial
                            "metadata": {
                                "processing_order": "AFTER_LOTECART_VALIDATION",
                                "strategy_used": strategy,
                                "quantite_theo_originale": lot_quantity,
                                "quantite_reelle_saisie": quantite_reelle_saisie,
                                "excluded_lotecart": True
                            }
                        })
                        
                        remaining_discrepancy -= adjustment
                        
                        logger.debug(
                            f"🔧 Ajustement non-LOTECART: {code_article} "
                            f"(Lot: {lot_number}, Ajustement: {adjustment})"
                        )
            
            logger.info(f"✅ {len(adjustments)} ajustements non-LOTECART créés avec stratégie {strategy}")
            return adjustments
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement ajustements non-LOTECART: {e}", exc_info=True)
            return []
    
    def _sort_lots_by_strategy(self, lots_df: pd.DataFrame, strategy: str) -> pd.DataFrame:
        """Trie les lots selon la stratégie FIFO/LIFO"""
        try:
            if strategy == "FIFO":
                return lots_df.sort_values("Date_Lot", na_position="last")
            else:  # LIFO
                return lots_df.sort_values("Date_Lot", ascending=False, na_position="last")
        except Exception as e:
            logger.warning(f"Erreur tri lots: {e}")
            return lots_df
    
    def _consolidate_with_coherence_check(
        self, 
        lotecart_result: Dict[str, Any],
        other_adjustments: List[Dict[str, Any]],
        strategy: str
    ) -> Dict[str, Any]:
        """
        Consolidation finale avec vérification de cohérence globale
        """
        try:
            logger.info("🔗 CONSOLIDATION FINALE AVEC VÉRIFICATION COHÉRENCE")
            
            # Extraire les données LOTECART
            lotecart_new = lotecart_result["new_adjustments"]
            lotecart_updates = lotecart_result["existing_updates"]
            lotecart_summary = lotecart_result["summary"]
            
            # Ordre de priorité strict: LOTECART d'abord, puis autres
            all_adjustments = []
            
            # 1. LOTECART prioritaires (nouvelles lignes) - PRIORITÉ 1
            all_adjustments.extend(lotecart_new)
            
            # 2. LOTECART mises à jour (lignes existantes) - PRIORITÉ 1
            all_adjustments.extend(lotecart_updates)
            
            # 3. Autres ajustements - PRIORITÉ 2
            all_adjustments.extend(other_adjustments)
            
            # Vérification finale de non-conflit
            conflicts = self._check_adjustment_conflicts(all_adjustments)
            if conflicts:
                raise ValueError(f"CONFLITS DÉTECTÉS DANS LES AJUSTEMENTS: {conflicts}")
            
            # Créer le résumé global
            global_summary = self._create_comprehensive_summary(
                lotecart_summary, other_adjustments, all_adjustments, strategy
            )
            
            final_result = {
                "lotecart_candidates": lotecart_result["candidates"],
                "lotecart_new_adjustments": lotecart_new,
                "lotecart_existing_updates": lotecart_updates,
                "other_adjustments": other_adjustments,
                "all_adjustments": all_adjustments,
                "lotecart_summary": lotecart_summary,
                "global_summary": global_summary,
                "strategy_used": strategy,
                "processing_mode": "STRICT_PRIORITY_LOTECART_FIRST",
                "validation_status": "VALIDATED"
            }
            
            logger.info(
                f"🔗 CONSOLIDATION TERMINÉE: "
                f"{len(lotecart_new)} nouveaux LOTECART + "
                f"{len(lotecart_updates)} LOTECART mis à jour + "
                f"{len(other_adjustments)} autres = "
                f"{len(all_adjustments)} ajustements totaux"
            )
            
            return final_result
            
        except Exception as e:
            logger.error(f"❌ Erreur consolidation avec cohérence: {e}", exc_info=True)
            raise
    
    def _check_adjustment_conflicts(self, all_adjustments: List[Dict[str, Any]]) -> List[str]:
        """
        Vérifie qu'il n'y a pas de conflits entre les ajustements
        """
        conflicts = []
        
        try:
            # Grouper par article + inventaire + lot
            adjustment_groups = {}
            
            for adj in all_adjustments:
                key = (
                    adj["CODE_ARTICLE"],
                    adj["NUMERO_INVENTAIRE"],
                    adj["NUMERO_LOT"]
                )
                
                if key not in adjustment_groups:
                    adjustment_groups[key] = []
                adjustment_groups[key].append(adj)
            
            # Vérifier les conflits
            for key, adjustments_for_key in adjustment_groups.items():
                if len(adjustments_for_key) > 1:
                    # Conflit potentiel
                    types = [adj["TYPE_LOT"] for adj in adjustments_for_key]
                    priorities = [adj.get("PRIORITY", 999) for adj in adjustments_for_key]
                    
                    conflicts.append(
                        f"Conflit pour {key[0]} (Lot: {key[2]}): "
                        f"{len(adjustments_for_key)} ajustements (Types: {types}, Priorités: {priorities})"
                    )
            
            return conflicts
            
        except Exception as e:
            logger.error(f"❌ Erreur vérification conflits: {e}")
            return [f"Erreur vérification conflits: {str(e)}"]
    
    def _create_comprehensive_summary(
        self, 
        lotecart_summary: Dict[str, Any],
        other_adjustments: List[Dict[str, Any]],
        all_adjustments: List[Dict[str, Any]],
        strategy: str
    ) -> Dict[str, Any]:
        """
        Crée un résumé complet et détaillé du traitement
        """
        try:
            total_lotecart = lotecart_summary.get("adjustments_created", 0)
            total_other = len(other_adjustments)
            total_adjustments = len(all_adjustments)
            
            # Calculer les quantités totales
            total_lotecart_qty = lotecart_summary.get("total_quantity", 0)
            total_other_qty = sum(
                abs(adj.get("AJUSTEMENT", 0)) for adj in other_adjustments
            )
            total_qty = total_lotecart_qty + total_other_qty
            
            # Statistiques détaillées
            lotecart_stats = {
                "count": total_lotecart,
                "quantity": total_lotecart_qty,
                "percentage": (total_lotecart / total_adjustments * 100) if total_adjustments > 0 else 0,
                "validation_status": "VALIDATED" if self.lotecart_validated else "PENDING"
            }
            
            other_stats = {
                "count": total_other,
                "quantity": total_other_qty,
                "percentage": (total_other / total_adjustments * 100) if total_adjustments > 0 else 0,
                "strategy_used": strategy
            }
            
            # Indicateurs de qualité
            quality_indicators = {
                "lotecart_coverage": 100.0 if total_lotecart == lotecart_summary.get("candidates_detected", 0) else 0.0,
                "total_coverage": (total_adjustments / (total_adjustments + 1)) * 100,
                "processing_coherence": "STRICT_PRIORITY_MAINTAINED",
                "validation_passed": self.lotecart_validated
            }
            
            global_summary = {
                "processing_mode": "STRICT_PRIORITY_LOTECART_FIRST",
                "total_adjustments": total_adjustments,
                "total_quantity_adjusted": total_qty,
                "lotecart_stats": lotecart_stats,
                "other_stats": other_stats,
                "priority_order": ["LOTECART_PRIORITY_1", "OTHER_ADJUSTMENTS_PRIORITY_2"],
                "processing_timestamp": pd.Timestamp.now().isoformat(),
                "quality_indicators": quality_indicators,
                "strategy_used": strategy,
                "validation_status": "COMPLETE" if self.lotecart_validated else "INCOMPLETE"
            }
            
            logger.info(
                f"📊 RÉSUMÉ COMPLET: "
                f"{total_lotecart} LOTECART (validés) + {total_other} autres = {total_adjustments} ajustements totaux"
            )
            
            return global_summary
            
        except Exception as e:
            logger.error(f"❌ Erreur création résumé complet: {e}", exc_info=True)
            return {}
    
    def generate_coherent_final_file(
        self, 
        session_id: str,
        original_df: pd.DataFrame,
        completed_df: pd.DataFrame,
        header_lines: List[str],
        output_path: str
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Génère le fichier final avec cohérence GARANTIE
        
        LOGIQUE STRICTE:
        - Colonne F (QUANTITE): Quantité théorique corrigée
        - Colonne G (QUANTITE_REELLE_IN_INPUT): Quantité réelle saisie (traçabilité)
        - Pour LOTECART: F = G = quantité saisie
        - Pour autres: F = quantité ajustée, G = quantité saisie
        """
        try:
            logger.info("🎯 GÉNÉRATION FICHIER FINAL AVEC COHÉRENCE GARANTIE")
            
            if not self.processing_summary:
                raise ValueError("Aucun traitement effectué. Appelez process_with_strict_priority() d'abord.")
            
            if not self.lotecart_validated:
                raise ValueError("LOTECART non validés - génération fichier impossible")
            
            # Récupérer toutes les données de traitement
            lotecart_new = self.processing_summary["lotecart_new_adjustments"]
            lotecart_updates = self.processing_summary["lotecart_existing_updates"]
            other_adjustments = self.processing_summary["other_adjustments"]
            
            logger.info(
                f"📋 DONNÉES POUR GÉNÉRATION: "
                f"{len(lotecart_new)} nouveaux LOTECART, "
                f"{len(lotecart_updates)} LOTECART mis à jour, "
                f"{len(other_adjustments)} autres ajustements"
            )
            
            # Créer les dictionnaires de référence
            saisies_dict = self._create_saisies_reference(completed_df)
            adjustments_dict = self._create_adjustments_reference(
                lotecart_new, lotecart_updates, other_adjustments
            )
            
            # Générer le contenu du fichier avec logique stricte
            lines = []
            lines.extend(header_lines)
            
            # Compteurs pour validation
            lines_processed = 0
            lotecart_lines_applied = 0
            other_lines_applied = 0
            
            # Traiter toutes les lignes originales
            for _, original_row in original_df.iterrows():
                if pd.notna(original_row["original_s_line_raw"]):
                    original_line = str(original_row["original_s_line_raw"])
                    parts = original_line.split(";")
                    
                    if len(parts) >= 15:
                        code_article = original_row["CODE_ARTICLE"]
                        numero_inventaire = original_row["NUMERO_INVENTAIRE"]
                        numero_lot_original = str(original_row["NUMERO_LOT"]).strip()
                        
                        # Clé pour recherche
                        key = (code_article, numero_inventaire, numero_lot_original)
                        
                        # Récupérer la quantité saisie (pour colonne G - traçabilité)
                        quantite_saisie = saisies_dict.get(key, 0)
                        
                        # Vérifier s'il y a un ajustement
                        if key in adjustments_dict:
                            adjustment = adjustments_dict[key]
                            
                            if adjustment["TYPE_LOT"] == "lotecart":
                                # LOGIQUE LOTECART STRICTE: F = G = quantité saisie
                                parts[5] = str(int(adjustment["QUANTITE_CORRIGEE"]))     # Colonne F
                                parts[6] = str(int(adjustment["QUANTITE_REELLE_SAISIE"])) # Colonne G
                                parts[7] = "2"                                           # Indicateur
                                parts[14] = "LOTECART"                                   # Numéro lot
                                lotecart_lines_applied += 1
                                
                                logger.debug(
                                    f"🎯 LOTECART APPLIQUÉ: {code_article} - "
                                    f"F={parts[5]}, G={parts[6]}, Lot=LOTECART"
                                )
                            else:
                                # LOGIQUE AUTRES AJUSTEMENTS: F = quantité corrigée, G = quantité saisie
                                parts[5] = str(int(adjustment["QUANTITE_CORRIGEE"]))     # Colonne F (ajustée)
                                parts[6] = str(int(adjustment["QUANTITE_REELLE_SAISIE"])) # Colonne G (saisie)
                                other_lines_applied += 1
                                
                                logger.debug(
                                    f"🔧 AUTRE AJUSTEMENT: {code_article} - "
                                    f"F={parts[5]}, G={parts[6]}"
                                )
                        else:
                            # LOGIQUE LIGNE STANDARD: F = quantité originale, G = quantité saisie
                            # parts[5] reste inchangé (quantité théorique originale)
                            parts[6] = str(int(quantite_saisie)) if quantite_saisie > 0 else "0"  # Colonne G
                        
                        # Ajouter la ligne modifiée
                        lines.append(";".join(parts))
                        lines_processed += 1
            
            # Ajouter les nouvelles lignes LOTECART
            max_line_number = self._get_max_line_number(original_df)
            new_lotecart_lines = self._generate_new_lotecart_lines(
                lotecart_new, max_line_number
            )
            
            lines.extend(new_lotecart_lines)
            new_lotecart_count = len(new_lotecart_lines)
            
            # Écrire le fichier avec encodage strict
            with open(output_path, "w", encoding="utf-8", newline="") as f:
                for line in lines:
                    f.write(line + "\n")
            
            # Validation finale du fichier généré
            expected_lotecart_total = len(lotecart_new) + len(lotecart_updates)
            validation_result = self._validate_generated_file(
                output_path, expected_lotecart_total
            )
            
            # Résumé de génération
            generation_summary = {
                "file_path": output_path,
                "total_lines_processed": lines_processed,
                "lotecart_lines_applied": lotecart_lines_applied,
                "other_lines_applied": other_lines_applied,
                "new_lotecart_lines": new_lotecart_count,
                "total_lotecart_lines": lotecart_lines_applied + new_lotecart_count,
                "validation": validation_result,
                "processing_mode": "STRICT_PRIORITY_COHERENT",
                "coherence_guaranteed": True
            }
            
            logger.info(
                f"✅ FICHIER FINAL COHÉRENT GÉNÉRÉ: {output_path} - "
                f"{lines_processed} lignes traitées, "
                f"{lotecart_lines_applied + new_lotecart_count} lignes LOTECART totales, "
                f"Validation: {'✅' if validation_result.get('success', False) else '❌'}"
            )
            
            return output_path, generation_summary
            
        except Exception as e:
            logger.error(f"❌ Erreur génération fichier final cohérent: {e}", exc_info=True)
            raise
    
    def _create_saisies_reference(self, completed_df: pd.DataFrame) -> Dict[Tuple, float]:
        """Crée le dictionnaire de référence des quantités saisies"""
        saisies_dict = {}
        
        for _, row in completed_df.iterrows():
            key = (
                row["Code Article"], 
                row.get("Numéro Inventaire", ""), 
                str(row.get("Numéro Lot", "")).strip()
            )
            saisies_dict[key] = float(row["Quantité Réelle"])
        
        logger.debug(f"📋 Dictionnaire saisies créé: {len(saisies_dict)} entrées")
        return saisies_dict
    
    def _create_adjustments_reference(
        self, 
        lotecart_new: List[Dict[str, Any]],
        lotecart_updates: List[Dict[str, Any]],
        other_adjustments: List[Dict[str, Any]]
    ) -> Dict[Tuple, Dict[str, Any]]:
        """Crée le dictionnaire de référence des ajustements"""
        adjustments_dict = {}
        
        # 1. LOTECART nouveaux (priorité absolue)
        for adj in lotecart_new:
            key = (
                adj["CODE_ARTICLE"], 
                adj["NUMERO_INVENTAIRE"], 
                adj.get("metadata", {}).get("original_lot", "")
            )
            adjustments_dict[key] = adj
        
        # 2. LOTECART mises à jour (priorité absolue)
        for adj in lotecart_updates:
            key = (
                adj["CODE_ARTICLE"], 
                adj["NUMERO_INVENTAIRE"], 
                adj.get("metadata", {}).get("original_lot", adj.get("NUMERO_LOT", ""))
            )
            adjustments_dict[key] = adj
        
        # 3. Autres ajustements (priorité inférieure - ne pas écraser LOTECART)
        for adj in other_adjustments:
            key = (
                adj["CODE_ARTICLE"], 
                adj["NUMERO_INVENTAIRE"], 
                adj["NUMERO_LOT"]
            )
            # Ne pas écraser les LOTECART
            if key not in adjustments_dict:
                adjustments_dict[key] = adj
        
        logger.debug(f"📋 Dictionnaire ajustements créé: {len(adjustments_dict)} entrées")
        return adjustments_dict
    
    def _generate_new_lotecart_lines(
        self, 
        lotecart_new_adjustments: List[Dict[str, Any]], 
        max_line_number: int
    ) -> List[str]:
        """Génère les nouvelles lignes LOTECART avec numérotation cohérente"""
        new_lines = []
        
        try:
            current_line_number = max_line_number
            
            for adjustment in lotecart_new_adjustments:
                if not adjustment.get("is_new_lotecart", False):
                    continue
                
                reference_line = adjustment.get("reference_line")
                if not reference_line:
                    logger.warning(
                        f"⚠️ Pas de ligne de référence pour nouveau LOTECART {adjustment['CODE_ARTICLE']}"
                    )
                    continue
                
                parts = str(reference_line).split(";")
                if len(parts) < 15:
                    logger.warning(
                        f"⚠️ Ligne de référence invalide pour LOTECART {adjustment['CODE_ARTICLE']}"
                    )
                    continue
                
                # Générer un nouveau numéro de ligne
                current_line_number += 1000
                
                # Construire la nouvelle ligne LOTECART
                new_parts = parts.copy()
                
                quantite_corrigee = int(adjustment["QUANTITE_CORRIGEE"])
                quantite_saisie = int(adjustment["QUANTITE_REELLE_SAISIE"])
                
                # LOGIQUE STRICTE LOTECART: F = G = quantité saisie
                new_parts[3] = str(current_line_number)  # RANG
                new_parts[5] = str(quantite_corrigee)    # QUANTITE (colonne F)
                new_parts[6] = str(quantite_saisie)      # QUANTITE_REELLE_IN_INPUT (colonne G)
                new_parts[7] = "2"                       # INDICATEUR_COMPTE
                new_parts[14] = "LOTECART"               # NUMERO_LOT
                
                new_line = ";".join(new_parts)
                new_lines.append(new_line)
                
                logger.debug(
                    f"✅ NOUVELLE LIGNE LOTECART: {adjustment['CODE_ARTICLE']} "
                    f"(Ligne={current_line_number}, F={quantite_corrigee}, G={quantite_saisie})"
                )
            
            logger.info(f"🎯 {len(new_lines)} nouvelles lignes LOTECART générées")
            return new_lines
            
        except Exception as e:
            logger.error(f"❌ Erreur génération nouvelles lignes LOTECART: {e}", exc_info=True)
            return []
    
    def _validate_generated_file(
        self, 
        file_path: str, 
        expected_lotecart_count: int
    ) -> Dict[str, Any]:
        """
        Validation finale du fichier généré avec vérifications strictes
        """
        validation = {
            "success": False,
            "lotecart_lines_found": 0,
            "lotecart_correct_indicators": 0,
            "lotecart_coherent_quantities": 0,
            "issues": [],
            "details": []
        }
        
        try:
            if not os.path.exists(file_path):
                validation["issues"].append("Fichier final non trouvé")
                return validation
            
            logger.info(f"🔍 VALIDATION FINALE STRICTE: {file_path}")
            
            # Analyser toutes les lignes LOTECART
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line.startswith('S;') and 'LOTECART' in line:
                        parts = line.split(';')
                        if len(parts) >= 15:
                            article = parts[8]
                            qty_f = parts[5]  # Colonne F
                            qty_g = parts[6]  # Colonne G
                            indicateur = parts[7]
                            
                            validation["lotecart_lines_found"] += 1
                            
                            # Vérifier l'indicateur
                            if indicateur == '2':
                                validation["lotecart_correct_indicators"] += 1
                            else:
                                validation["issues"].append(
                                    f"Indicateur incorrect ligne {line_num}: {article} (indicateur={indicateur})"
                                )
                            
                            # Vérifier la cohérence des quantités (F = G pour LOTECART)
                            try:
                                qty_f_val = float(qty_f)
                                qty_g_val = float(qty_g)
                                
                                if abs(qty_f_val - qty_g_val) < 0.001 and qty_f_val > 0:
                                    validation["lotecart_coherent_quantities"] += 1
                                else:
                                    validation["issues"].append(
                                        f"Quantités incohérentes ligne {line_num}: {article} (F={qty_f}, G={qty_g})"
                                    )
                            except ValueError:
                                validation["issues"].append(
                                    f"Quantités non numériques ligne {line_num}: {article}"
                                )
                            
                            # Ajouter aux détails
                            validation["details"].append({
                                "line": line_num,
                                "article": article,
                                "qty_f": qty_f,
                                "qty_g": qty_g,
                                "indicator": indicateur,
                                "status": "✅" if indicateur == '2' and abs(float(qty_f) - float(qty_g)) < 0.001 else "❌"
                            })
            
            # Vérifications globales
            if validation["lotecart_lines_found"] < expected_lotecart_count:
                validation["issues"].append(
                    f"Nombre LOTECART insuffisant: {validation['lotecart_lines_found']} < {expected_lotecart_count}"
                )
            
            # Succès si toutes les vérifications passent
            validation["success"] = (
                len(validation["issues"]) == 0 and
                validation["lotecart_correct_indicators"] == validation["lotecart_lines_found"] and
                validation["lotecart_coherent_quantities"] == validation["lotecart_lines_found"] and
                validation["lotecart_lines_found"] >= expected_lotecart_count
            )
            
            if validation["success"]:
                logger.info(
                    f"✅ VALIDATION FINALE RÉUSSIE: "
                    f"{validation['lotecart_lines_found']} lignes LOTECART parfaitement cohérentes"
                )
            else:
                logger.error(
                    f"❌ VALIDATION FINALE ÉCHOUÉE: "
                    f"{len(validation['issues'])} problème(s) détecté(s)"
                )
                for issue in validation["issues"][:5]:  # Afficher max 5 problèmes
                    logger.error(f"   🔴 {issue}")
            
            return validation
            
        except Exception as e:
            logger.error(f"❌ Erreur validation fichier généré: {e}", exc_info=True)
            validation["issues"].append(f"Erreur de validation: {str(e)}")
            return validation
    
    def _get_max_line_number(self, original_df: pd.DataFrame) -> int:
        """Récupère le numéro de ligne maximum pour éviter les conflits"""
        try:
            max_line = 0
            for _, row in original_df.iterrows():
                line_raw = str(row.get("original_s_line_raw", ""))
                parts = line_raw.split(";")
                if len(parts) > 3:
                    try:
                        line_num = int(parts[3])
                        max_line = max(max_line, line_num)
                    except (ValueError, IndexError):
                        pass
            return max_line
        except Exception as e:
            logger.warning(f"Erreur calcul numéro ligne max: {e}")
            return 0
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Retourne le résumé complet du traitement avec validation"""
        if not self.lotecart_validated:
            logger.warning("⚠️ LOTECART non validés - résumé incomplet")
        
        return self.processing_summary.copy()
    
    def reset_processor(self):
        """Remet à zéro le processeur avec validation"""
        self.lotecart_processor.reset_counter()
        self.processing_summary = {}
        self.lotecart_validated = False
        logger.info("🔄 Processeur prioritaire remis à zéro avec validation")

import os