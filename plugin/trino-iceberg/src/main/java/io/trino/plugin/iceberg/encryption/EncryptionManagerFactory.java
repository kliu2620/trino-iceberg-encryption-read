/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.encryption;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptionManager;

import java.util.List;
import java.util.Map;

/**
 * Factory for creating Iceberg {@link EncryptionManager} instances based on table properties
 * and the table's encryption keys. Returns
 * {@link org.apache.iceberg.encryption.PlaintextEncryptionManager} when the table is not
 * encrypted, or an encryption-capable manager when encryption is configured.
 *
 * <p>Iceberg 1.11.0 splits encryption material between table properties (which carry the
 * KMS-side key id and DEK length) and the catalog-managed list of {@link EncryptedKey}s
 * (which carry e.g. the manifest list key wrapped by the table key). Both are required to
 * read tables whose manifest list is encrypted.
 */
public interface EncryptionManagerFactory
{
    EncryptionManager create(Map<String, String> tableProperties, List<EncryptedKey> encryptionKeys);

    /**
     * Convenience overload for callers (e.g. system $files table reads) that don't have
     * access to the catalog-managed encryption keys list. The returned manager can still
     * decrypt per-file Parquet data files because their DEKs are wrapped per file via
     * key_metadata, but it cannot decrypt the manifest list.
     */
    default EncryptionManager create(Map<String, String> tableProperties)
    {
        return create(tableProperties, ImmutableList.of());
    }
}
