package io.bisq.common.storage;

import io.bisq.common.persistance.Persistable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/*
 * This file is part of bisq.
 *
 * bisq is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at
 * your option) any later version.
 *
 * bisq is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with bisq. If not, see <http://www.gnu.org/licenses/>.
 */
public interface FileManager {
    void setArguments(File dir, File storageFile, long delay);

    void saveNow(Persistable serializable);

    void saveLater(Persistable serializable);

    void saveLater(Persistable serializable, long delayInMilli);

    Persistable read(File file) throws IOException, ClassNotFoundException;

    void removeFile(String fileName);

    void removeAndBackupFile(String fileName) throws IOException;

    void backupFile(String fileName, int numMaxBackupFiles) throws IOException;
}
