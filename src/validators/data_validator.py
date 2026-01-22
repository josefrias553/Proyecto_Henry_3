"""
Módulo de Validador de Datos.
Proporciona validación básica de datos antes de la carga a S3.
"""

from typing import List, Dict, Any, Set
import json

from src.logger import logger


class DataValidator:
    """
    Validador para datos extraídos.
    
    Realiza validaciones básicas:
    - Verificación de datos no vacíos
    - Verificación de serialización JSON
    - Verificación de consistencia de esquema (mismos campos entre registros)
    """
    
    @staticmethod
    def validate_not_empty(data: List[Dict[str, Any]]) -> bool:
        """
        Valida que los datos no estén vacíos.
        
        Args:
            data: Datos a validar
            
        Returns:
            bool: True si es válido, False de lo contrario
        """
        if not data:
            logger.error("Validación fallida: Los datos están vacíos")
            return False
        
        if not isinstance(data, list):
            logger.error(f"Validación fallida: Los datos no son una lista, se obtuvo {type(data)}")
            return False
        
        if len(data) == 0:
            logger.error("Validación fallida: La lista de datos está vacía")
            return False
        
        logger.info(f"✓ Datos no vacíos: {len(data)} registros")
        return True
    
    @staticmethod
    def validate_json_serializable(data: List[Dict[str, Any]]) -> bool:
        """
        Valida que los datos puedan serializarse a JSON.
        
        Args:
            data: Datos a validar
            
        Returns:
            bool: True si es válido, False de lo contrario
        """
        try:
            json.dumps(data)
            logger.info("✓ Datos serializables a JSON")
            return True
        except (TypeError, ValueError) as e:
            logger.error(f"Validación fallida: Los datos no son serializables a JSON - {str(e)}")
            return False
    
    @staticmethod
    def validate_schema_consistency(
        data: List[Dict[str, Any]],
        entity: str,
        strict: bool = False
    ) -> bool:
        """
        Valida que todos los registros tengan una estructura consistente.
        
        Args:
            data: Datos a validar
            entity: Nombre de la entidad (para logging)
            strict: Si es True, todos los registros deben tener claves idénticas
                   Si es False, verifica que la mayoría de registros compartan campos comunes
            
        Returns:
            bool: True si es válido, False de lo contrario
        """
        if not data:
            return True  # Datos vacíos son trivialmente consistentes
        
        # Recolectar todos los sets de campos
        field_sets: List[Set[str]] = []
        for item in data:
            if not isinstance(item, dict):
                logger.warning(f"Ítem que no es dict encontrado: {type(item)}")
                continue
            field_sets.append(set(item.keys()))
        
        if not field_sets:
            logger.error("Validación fallida: No se encontraron diccionarios válidos en los datos")
            return False
        
        # Obtener set de campos más común
        all_fields = set.union(*field_sets)
        common_fields = set.intersection(*field_sets)
        
        logger.info(f"Análisis de campos para {entity}:")
        logger.info(f"  - Total campos únicos: {len(all_fields)}")
        logger.info(f"  - Campos comunes: {len(common_fields)}")
        logger.info(f"  - Nombres de campos: {sorted(common_fields)}")
        
        if strict:
            # Todos los registros deben tener campos idénticos
            if len(set(map(frozenset, field_sets))) > 1:
                logger.error("Validación fallida: Los registros tienen diferentes sets de campos (modo estricto)")
                return False
            logger.info("✓ El esquema es consistente (estricto)")
            return True
        else:
            # Al menos el 50% de los campos deberían ser comunes
            consistency_ratio = len(common_fields) / len(all_fields) if all_fields else 0
            
            if consistency_ratio < 0.5:
                logger.warning(
                    f"Baja consistencia de esquema: {consistency_ratio:.1%} "
                    f"({len(common_fields)}/{len(all_fields)} campos comunes)"
                )
            
            # Verificar que al menos algunos campos sean comunes
            if len(common_fields) == 0:
                logger.error("Validación fallida: No hay campos comunes entre registros")
                return False
            
            logger.info(f"✓ Consistencia de esquema: {consistency_ratio:.1%}")
            return True
    
    @classmethod
    def validate_all(
        cls,
        data: List[Dict[str, Any]],
        entity: str,
        strict_schema: bool = False
    ) -> bool:
        """
        Ejecuta todas las validaciones.
        
        Args:
            data: Datos a validar
            entity: Nombre de la entidad
            strict_schema: Si se debe usar validación estricta de esquema
            
        Returns:
            bool: True si todas las validaciones pasan
        """
        logger.info(f"Ejecutando validaciones para entidad: {entity}")
        
        validations = [
            cls.validate_not_empty(data),
            cls.validate_json_serializable(data),
            cls.validate_schema_consistency(data, entity, strict=strict_schema)
        ]
        
        if all(validations):
            logger.info(f"✓ Todas las validaciones pasaron para {entity}")
            return True
        else:
            logger.error(f"✗ Algunas validaciones fallaron para {entity}")
            return False
