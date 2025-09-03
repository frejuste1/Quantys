import pandas as pd
import logging
from typing import Tuple, List, Dict, Any, Optional
from services.lotecart_processor import LotecartProcessor

logger = logging.getLogger(__name__)

class PriorityProcessor:
    """
    Processeur avec gestion des priorités:
    1. LOTECART (priorité absolue)
    2. Autres ajustements (après LOTECART)
    3. Génération fichier final (avec toutes les corrections)
    """
    
    def __init__(self):
        self.lotecart_processor = LotecartProcessor()
        self.processing_summary = {}
    
    def process_with_priority(
        self, 
        completed_df: pd.DataFrame, 
        original_df: pd.DataFrame,
        strategy: str = "FIFO"
    ) -> Dict[str, Any]:
        """
        Traite les données avec priorisation LOTECART
        
        Args:
            completed_df: DataFrame du template complété
            original_df: DataFrame des données originales
            strategy: Stratégie de répartition pour les autres lots
            
        Returns:
            Dictionnaire avec tous les résultats de traitement
        """
        try:
            logger.info("🚀 DÉBUT DU TRAITEMENT AVEC PRIORISATION")
            
            # ÉTAPE 1: Traitement prioritaire des LOTECART
            logger.info("📍 ÉTAPE 1: TRAITEMENT PRIORITAIRE LOTECART")
            
            lotecart_candidates, lotecart_adjustments, lotecart_summary = (
                self.lotecart_processor.detect_and_process_lotecart_priority(
                    completed_df, original_df
                )
            )
            
            # Mise à jour des lignes existantes avec quantité théorique = 0
            lotecart_updates = self.lotecart_processor.update_existing_lotecart_lines(
                original_df, completed_df
            )
            
            # ÉTAPE 2: Traitement des autres ajustements (non-LOTECART)
            logger.info("📍 ÉTAPE 2: TRAITEMENT DES AUTRES AJUSTEMENTS")
            
            other_adjustments = self._process_non_lotecart_adjustments(
                completed_df, original_df, lotecart_candidates, strategy
            )
            
            # ÉTAPE 3: Consolidation de tous les ajustements
            logger.info("📍 ÉTAPE 3: CONSOLIDATION DES AJUSTEMENTS")
            
            all_adjustments = self._consolidate_adjustments(
                lotecart_adjustments, lotecart_updates, other_adjustments
            )
            
            # ÉTAPE 4: Préparation du résumé global
            global_summary = self._create_global_summary(
                lotecart_summary, other_adjustments, all_adjustments
            )
            
            # Sauvegarder le résumé pour la génération du fichier final
            self.processing_summary = {
                "lotecart_candidates": lotecart_candidates,
                "lotecart_adjustments": lotecart_adjustments,
                "lotecart_updates": lotecart_updates,
                "other_adjustments": other_adjustments,
                "all_adjustments": all_adjustments,
                "lotecart_summary": lotecart_summary,
                "global_summary": global_summary,
                "strategy_used": strategy
            }
            
            logger.info("✅ TRAITEMENT AVEC PRIORISATION TERMINÉ")
            
            return self.processing_summary
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement avec priorisation: {e}", exc_info=True)
            raise
    
    def _process_non_lotecart_adjustments(
        self, 
        completed_df: pd.DataFrame, 
        original_df: pd.DataFrame,
        lotecart_candidates: pd.DataFrame,
        strategy: str
    ) -> List[Dict[str, Any]]:
        """
        Traite les ajustements non-LOTECART après avoir exclu les LOTECART
        
        Args:
            completed_df: DataFrame du template complété
            original_df: DataFrame des données originales
            lotecart_candidates: DataFrame des candidats LOTECART (à exclure)
            strategy: Stratégie de répartition (FIFO/LIFO)
            
        Returns:
            Liste des ajustements non-LOTECART
        """
        try:
            # Calculer les écarts en excluant les LOTECART
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
            
            # Exclure les LOTECART des écarts à traiter
            if not lotecart_candidates.empty:
                lotecart_articles = set()
                for _, candidate in lotecart_candidates.iterrows():
                    key = (
                        candidate["Code Article"],
                        candidate.get("Numéro Inventaire", ""),
                        str(candidate.get("Numéro Lot", "")).strip()
                    )
                    lotecart_articles.add(key)
                
                # Filtrer les écarts non-LOTECART
                non_lotecart_mask = completed_clean.apply(
                    lambda row: (
                        row["Code Article"],
                        row.get("Numéro Inventaire", ""),
                        str(row.get("Numéro Lot", "")).strip()
                    ) not in lotecart_articles,
                    axis=1
                )
                
                discrepancies_df = completed_clean[
                    non_lotecart_mask & (completed_clean["Écart"] != 0)
                ].copy()
            else:
                # Pas de LOTECART, traiter tous les écarts
                discrepancies_df = completed_clean[completed_clean["Écart"] != 0].copy()
            
            if discrepancies_df.empty:
                logger.info("ℹ️ Aucun écart non-LOTECART à traiter")
                return []
            
            logger.info(f"🔧 Traitement de {len(discrepancies_df)} écarts non-LOTECART avec stratégie {strategy}")
            
            # Distribuer les écarts selon la stratégie
            adjustments = []
            
            for _, discrepancy_row in discrepancies_df.iterrows():
                code_article = discrepancy_row["Code Article"]
                numero_inventaire = discrepancy_row.get("Numéro Inventaire", "")
                ecart = discrepancy_row["Écart"]
                quantite_reelle_saisie = discrepancy_row["Quantité Réelle"]
                
                if abs(ecart) < 0.001:
                    continue
                
                # Trouver les lots pour cet article
                if numero_inventaire:
                    article_lots = original_df[
                        (original_df["CODE_ARTICLE"] == code_article) &
                        (original_df["NUMERO_INVENTAIRE"] == numero_inventaire)
                    ].copy()
                else:
                    article_lots = original_df[
                        original_df["CODE_ARTICLE"] == code_article
                    ].copy()
                
                if article_lots.empty:
                    continue
                
                # Trier selon la stratégie
                article_lots = self._sort_lots_by_strategy(article_lots, strategy)
                
                # Distribuer l'écart
                remaining_discrepancy = ecart
                
                for _, lot_row in article_lots.iterrows():
                    if abs(remaining_discrepancy) < 0.001:
                        break
                    
                    lot_quantity = float(lot_row["QUANTITE"])
                    lot_number = lot_row["NUMERO_LOT"] if lot_row["NUMERO_LOT"] else ""
                    
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
                            "metadata": {
                                "processing_order": "AFTER_LOTECART",
                                "strategy_used": strategy,
                                "quantite_theo_originale": lot_quantity,
                                "quantite_reelle_saisie": quantite_reelle_saisie
                            }
                        })
                        
                        remaining_discrepancy -= adjustment
            
            logger.info(f"✅ {len(adjustments)} ajustements non-LOTECART créés avec stratégie {strategy}")
            return adjustments
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement ajustements non-LOTECART: {e}", exc_info=True)
            return []
    
    def _sort_lots_by_strategy(self, lots_df: pd.DataFrame, strategy: str) -> pd.DataFrame:
        """Trie les lots selon la stratégie FIFO/LIFO"""
        try:
            if strategy == "FIFO":
                # Plus anciens d'abord
                return lots_df.sort_values("Date_Lot", na_position="last")
            else:  # LIFO
                # Plus récents d'abord
                return lots_df.sort_values("Date_Lot", ascending=False, na_position="last")
        except Exception as e:
            logger.warning(f"Erreur tri lots: {e}")
            return lots_df
    
    def _consolidate_adjustments(
        self, 
        lotecart_adjustments: List[Dict[str, Any]],
        lotecart_updates: List[Dict[str, Any]],
        other_adjustments: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Consolide tous les ajustements avec priorité LOTECART
        
        Args:
            lotecart_adjustments: Nouveaux ajustements LOTECART
            lotecart_updates: Mises à jour de lignes existantes LOTECART
            other_adjustments: Autres ajustements
            
        Returns:
            Liste consolidée avec priorité respectée
        """
        try:
            # Ordre de priorité: LOTECART d'abord, puis autres
            all_adjustments = []
            
            # 1. LOTECART prioritaires (nouvelles lignes)
            all_adjustments.extend(lotecart_adjustments)
            
            # 2. LOTECART mises à jour (lignes existantes)
            all_adjustments.extend(lotecart_updates)
            
            # 3. Autres ajustements
            all_adjustments.extend(other_adjustments)
            
            logger.info(
                f"🔗 CONSOLIDATION: "
                f"{len(lotecart_adjustments)} nouveaux LOTECART + "
                f"{len(lotecart_updates)} LOTECART mis à jour + "
                f"{len(other_adjustments)} autres = "
                f"{len(all_adjustments)} ajustements totaux"
            )
            
            return all_adjustments
            
        except Exception as e:
            logger.error(f"❌ Erreur consolidation ajustements: {e}", exc_info=True)
            return []
    
    def _create_global_summary(
        self, 
        lotecart_summary: Dict[str, Any],
        other_adjustments: List[Dict[str, Any]],
        all_adjustments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Crée un résumé global du traitement avec priorités
        
        Args:
            lotecart_summary: Résumé du traitement LOTECART
            other_adjustments: Liste des autres ajustements
            all_adjustments: Liste de tous les ajustements
            
        Returns:
            Dictionnaire avec le résumé global
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
            
            global_summary = {
                "processing_mode": "PRIORITY_BASED",
                "total_adjustments": total_adjustments,
                "lotecart_stats": {
                    "count": total_lotecart,
                    "quantity": total_lotecart_qty,
                    "percentage": (total_lotecart / total_adjustments * 100) if total_adjustments > 0 else 0
                },
                "other_stats": {
                    "count": total_other,
                    "quantity": total_other_qty,
                    "percentage": (total_other / total_adjustments * 100) if total_adjustments > 0 else 0
                },
                "priority_order": ["LOTECART", "OTHER_ADJUSTMENTS"],
                "processing_timestamp": pd.Timestamp.now().isoformat(),
                "quality_indicators": {
                    "lotecart_coverage": (total_lotecart / lotecart_summary.get("candidates_detected", 1)) * 100,
                    "total_coverage": (total_adjustments / (total_adjustments + 1)) * 100  # Approximation
                }
            }
            
            logger.info(
                f"📊 RÉSUMÉ GLOBAL: "
                f"{total_lotecart} LOTECART + {total_other} autres = {total_adjustments} ajustements"
            )
            
            return global_summary
            
        except Exception as e:
            logger.error(f"❌ Erreur création résumé global: {e}", exc_info=True)
            return {}
    
    def generate_priority_final_file(
        self, 
        session_id: str,
        original_df: pd.DataFrame,
        completed_df: pd.DataFrame,
        header_lines: List[str],
        output_path: str
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Génère le fichier final avec traitement prioritaire
        
        Args:
            session_id: ID de la session
            original_df: DataFrame des données originales
            completed_df: DataFrame du template complété
            header_lines: Lignes d'en-tête E et L
            output_path: Chemin de sortie du fichier
            
        Returns:
            Tuple (chemin_fichier, résumé_validation)
        """
        try:
            logger.info("🎯 GÉNÉRATION DU FICHIER FINAL AVEC PRIORITÉ LOTECART")
            
            if not self.processing_summary:
                raise ValueError("Aucun traitement prioritaire effectué. Appelez process_with_priority() d'abord.")
            
            # Récupérer les données du traitement
            lotecart_adjustments = self.processing_summary["lotecart_adjustments"]
            lotecart_updates = self.processing_summary["lotecart_updates"]
            other_adjustments = self.processing_summary["other_adjustments"]
            
            # Créer un dictionnaire des quantités saisies (pour colonne G)
            saisies_dict = {}
            for _, row in completed_df.iterrows():
                key = (
                    row["Code Article"], 
                    row.get("Numéro Inventaire", ""), 
                    str(row.get("Numéro Lot", "")).strip()
                )
                saisies_dict[key] = row["Quantité Réelle"]
            
            # Créer un dictionnaire de tous les ajustements
            all_adjustments_dict = {}
            
            # 1. Ajustements LOTECART (priorité 1)
            for adj in lotecart_adjustments + lotecart_updates:
                key = (
                    adj["CODE_ARTICLE"], 
                    adj["NUMERO_INVENTAIRE"], 
                    adj.get("metadata", {}).get("original_lot", adj.get("NUMERO_LOT", ""))
                )
                all_adjustments_dict[key] = adj
            
            # 2. Autres ajustements (priorité 2)
            for adj in other_adjustments:
                key = (
                    adj["CODE_ARTICLE"], 
                    adj["NUMERO_INVENTAIRE"], 
                    adj["NUMERO_LOT"]
                )
                # Ne pas écraser les LOTECART
                if key not in all_adjustments_dict:
                    all_adjustments_dict[key] = adj
            
            # Générer le contenu du fichier
            lines = []
            
            # Ajouter les en-têtes
            lines.extend(header_lines)
            
            # Traiter toutes les lignes originales
            lines_processed = 0
            lines_adjusted = 0
            lotecart_lines_updated = 0
            
            for _, original_row in original_df.iterrows():
                if pd.notna(original_row["original_s_line_raw"]):
                    original_line = str(original_row["original_s_line_raw"])
                    parts = original_line.split(";")
                    
                    if len(parts) >= 15:
                        code_article = original_row["CODE_ARTICLE"]
                        numero_inventaire = original_row["NUMERO_INVENTAIRE"]
                        numero_lot_original = str(original_row["NUMERO_LOT"]).strip()
                        
                        key = (code_article, numero_inventaire, numero_lot_original)
                        
                        # Récupérer la quantité saisie (pour colonne G)
                        quantite_saisie = saisies_dict.get(key, 0)
                        
                        # Vérifier s'il y a un ajustement
                        if key in all_adjustments_dict:
                            adjustment = all_adjustments_dict[key]
                            
                            # Appliquer l'ajustement selon le type
                            if adjustment["TYPE_LOT"] == "lotecart":
                                # LOTECART: F = G = quantité saisie
                                parts[5] = str(int(adjustment["QUANTITE_CORRIGEE"]))
                                parts[6] = str(int(adjustment["QUANTITE_REELLE_SAISIE"]))
                                parts[7] = "2"  # Indicateur
                                parts[14] = "LOTECART"  # Forcer le numéro de lot
                                lotecart_lines_updated += 1
                                
                                logger.debug(
                                    f"🎯 LOTECART appliqué: {code_article} - "
                                    f"F={parts[5]}, G={parts[6]}, Lot=LOTECART"
                                )
                            else:
                                # Ajustement normal: F = quantité corrigée, G = quantité saisie
                                parts[5] = str(int(adjustment["QUANTITE_CORRIGEE"]))
                                parts[6] = str(int(quantite_saisie))
                                
                                logger.debug(
                                    f"🔧 Ajustement normal: {code_article} - "
                                    f"F={parts[5]}, G={parts[6]}"
                                )
                            
                            lines_adjusted += 1
                        else:
                            # Ligne sans ajustement: F = quantité originale, G = quantité saisie
                            parts[6] = str(int(quantite_saisie)) if quantite_saisie > 0 else "0"
                        
                        # Ajouter la ligne modifiée
                        lines.append(";".join(parts))
                        lines_processed += 1
            
            # Ajouter les nouvelles lignes LOTECART
            max_line_number = self._get_max_line_number(original_df)
            new_lotecart_lines = self.lotecart_processor.generate_priority_lotecart_lines(
                lotecart_adjustments, max_line_number
            )
            
            lines.extend(new_lotecart_lines)
            new_lotecart_count = len(new_lotecart_lines)
            
            # Écrire le fichier
            with open(output_path, "w", encoding="utf-8", newline="") as f:
                for line in lines:
                    f.write(line + "\n")
            
            # Validation finale
            expected_lotecart_total = len(lotecart_adjustments) + len(lotecart_updates)
            validation_result = self.lotecart_processor.validate_final_lotecart_processing(
                output_path, expected_lotecart_total
            )
            
            # Résumé de génération
            generation_summary = {
                "file_path": output_path,
                "total_lines_processed": lines_processed,
                "lines_adjusted": lines_adjusted,
                "lotecart_lines_updated": lotecart_lines_updated,
                "new_lotecart_lines": new_lotecart_count,
                "total_lotecart_lines": lotecart_lines_updated + new_lotecart_count,
                "validation": validation_result,
                "processing_mode": "PRIORITY_BASED"
            }
            
            logger.info(
                f"✅ FICHIER FINAL GÉNÉRÉ AVEC PRIORITÉ: {output_path} - "
                f"{lines_processed} lignes traitées, "
                f"{lotecart_lines_updated + new_lotecart_count} lignes LOTECART totales"
            )
            
            return output_path, generation_summary
            
        except Exception as e:
            logger.error(f"❌ Erreur génération fichier final prioritaire: {e}", exc_info=True)
            raise
    
    def _get_max_line_number(self, original_df: pd.DataFrame) -> int:
        """Récupère le numéro de ligne maximum des données originales"""
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
        """Retourne le résumé complet du traitement"""
        return self.processing_summary.copy()
    
    def reset_processor(self):
        """Remet à zéro le processeur"""
        self.lotecart_processor.reset_counter()
        self.processing_summary = {}
        logger.info("🔄 Processeur prioritaire remis à zéro")