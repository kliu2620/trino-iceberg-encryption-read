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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.util.Map;

/**
 * Wraps {@link EncryptingFileIO} to delegate every {@code newInputFile} variant so encrypted
 * data files, delete files, manifests and manifest lists are decrypted transparently. In
 * Iceberg 1.11+ {@code EncryptingFileIO.properties()} is implemented properly so we forward
 * straight to the wrapper; the historical workaround for {@code properties()} is no longer
 * needed.
 */
public class EncryptionAwareFileIO
        implements FileIO
{
    private final EncryptingFileIO delegate;

    public EncryptionAwareFileIO(FileIO fileIo, EncryptionManager encryptionManager)
    {
        this.delegate = EncryptingFileIO.combine(fileIo, encryptionManager);
    }

    @Override
    public InputFile newInputFile(String path)
    {
        return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length)
    {
        return delegate.newInputFile(path, length);
    }

    @Override
    public InputFile newInputFile(DataFile file)
    {
        return delegate.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(DeleteFile file)
    {
        return delegate.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest)
    {
        return delegate.newInputFile(manifest);
    }

    @Override
    public InputFile newInputFile(ManifestListFile manifestList)
    {
        return delegate.newInputFile(manifestList);
    }

    @Override
    public OutputFile newOutputFile(String path)
    {
        return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path)
    {
        delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties()
    {
        return delegate.properties();
    }

    @Override
    public void initialize(Map<String, String> newProperties)
    {
        delegate.initialize(newProperties);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
