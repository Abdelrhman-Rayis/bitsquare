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

package io.bisq.core;

import com.google.inject.Inject;
import com.google.protobuf.Message;
import io.bisq.common.UserThread;
import io.bisq.common.io.LookAheadObjectInputStream;
import io.bisq.common.locale.*;
import io.bisq.common.persistance.Persistable;
import io.bisq.common.persistance.ProtobufferResolver;
import io.bisq.common.storage.FileManager;
import io.bisq.common.storage.FileUtil;
import io.bisq.common.storage.PlainTextWrapper;
import io.bisq.common.util.Utilities;
import io.bisq.core.btc.AddressEntry;
import io.bisq.core.btc.AddressEntryList ;
import io.bisq.core.user.BlockChainExplorer;
import io.bisq.core.user.Preferences;
import io.bisq.generated.protobuffer.PB;
import org.bitcoinj.core.Coin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileManagerImpl implements FileManager {
    private static final Logger log = LoggerFactory.getLogger(FileManagerImpl.class);

    private File dir;
    private File storageFile;
    private long delay;
    private final ScheduledThreadPoolExecutor executor;
    private final AtomicBoolean savePending;
    private final Callable<Void> saveFileTask;
    private Persistable serializable;
    @Inject
    private ProtobufferResolver protobufferResolver;
    @Inject
    private Preferences preferences;
    @Inject
    private AddressEntryList  AddressEntryList ;


    ///////////////////////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////////////////////

    //@Inject
    public FileManagerImpl() {
        /*
        this.protobufferResolver = protobufferResolver;
        this.preferences = preferences;
        this.AddressEntryList  = AddressEntryList ;
        */
        executor = Utilities.getScheduledThreadPoolExecutor("FileManagerImpl", 1, 10, 5);

        // File must only be accessed from the auto-save executor from now on, to avoid simultaneous access.
        savePending = new AtomicBoolean();

        saveFileTask = () -> {
            Thread.currentThread().setName("Save-file-task-" + new Random().nextInt(10000));
            // Runs in an auto save thread.
            if (!savePending.getAndSet(false)) {
                // Some other scheduled request already beat us to it.
                return null;
            }
            saveNowInternal(serializable);
            return null;
        };
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            UserThread.execute(FileManagerImpl.this::shutDown);
        }, "FileManagerImpl.ShutDownHook"));
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // API
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void setArguments(File dir, File storageFile, long delay) {
        this.dir = dir;
        this.storageFile = storageFile;
        this.delay = delay;
    }

    /**
     * Actually write the wallet file to disk, using an atomic rename when possible. Runs on the current thread.
     */
    @Override
    public void saveNow(Persistable serializable) {
        saveNowInternal(serializable);
    }

    /**
     * Queues up a save in the background. Useful for not very important wallet changes.
     */
    @Override
    public void saveLater(Persistable serializable) {
        saveLater(serializable, delay);
    }

    @Override
    public void saveLater(Persistable serializable, long delayInMilli) {
        this.serializable = serializable;

        if (savePending.getAndSet(true))
            return;   // Already pending.

        executor.schedule(saveFileTask, delayInMilli, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized Persistable read(File file) throws IOException, ClassNotFoundException {
        log.debug("read" + file);
        Optional<Persistable> persistable = Optional.empty();

        try (final FileInputStream fileInputStream = new FileInputStream(file)) {
            persistable = fromProto(PB.DiskEnvelope.parseFrom(fileInputStream));
        } catch (Throwable t) {
            log.error("Exception at proto read: " + t.getMessage() + " " + file.getName());
        }

        if(persistable.isPresent()) {
            log.error("Persistable found");
            return (Persistable) persistable.get();
        }

        try (final FileInputStream fileInputStream = new FileInputStream(file);
             final ObjectInputStream objectInputStream = new LookAheadObjectInputStream(fileInputStream, false)) {
            return (Persistable) objectInputStream.readObject();
        } catch (Throwable t) {
            log.error("Exception at read: " + t.getMessage());
            throw t;
        }
    }

    @Override
    public synchronized void removeFile(String fileName) {
        log.debug("removeFile" + fileName);
        File file = new File(dir, fileName);
        boolean result = file.delete();
        if (!result)
            log.warn("Could not delete file: " + file.toString());

        File backupDir = new File(Paths.get(dir.getAbsolutePath(), "backup").toString());
        if (backupDir.exists()) {
            File backupFile = new File(Paths.get(dir.getAbsolutePath(), "backup", fileName).toString());
            if (backupFile.exists()) {
                result = backupFile.delete();
                if (!result)
                    log.warn("Could not delete backupFile: " + file.toString());
            }
        }
    }


    /**
     * Shut down auto-saving.
     */
    void shutDown() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public synchronized void removeAndBackupFile(String fileName) throws IOException {
        File corruptedBackupDir = new File(Paths.get(dir.getAbsolutePath(), "backup_of_corrupted_data").toString());
        if (!corruptedBackupDir.exists())
            if (!corruptedBackupDir.mkdir())
                log.warn("make dir failed");

        File corruptedFile = new File(Paths.get(dir.getAbsolutePath(), "backup_of_corrupted_data", fileName).toString());
        renameTempFileToFile(storageFile, corruptedFile);
    }

    @Override
    public synchronized void backupFile(String fileName, int numMaxBackupFiles) throws IOException {
        FileUtil.rollingBackup(dir, fileName, numMaxBackupFiles);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Private
    ///////////////////////////////////////////////////////////////////////////////////////////

    private void saveNowInternal(Persistable serializable) {
        long now = System.currentTimeMillis();
        saveToFile(serializable, dir, storageFile);
        UserThread.execute(() -> log.trace("Save {} completed in {}msec", storageFile, System.currentTimeMillis() - now));
    }

    // TODO Sometimes we get a ConcurrentModificationException here
    private synchronized void saveToFile(Persistable serializable, File dir, File storageFile) {
        File tempFile = null;
        FileOutputStream fileOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        PrintWriter printWriter = null;

        // is it a protobuffer thing?

        Message message = null;
        try {
            message = ((Persistable) serializable).toProtobuf();
        } catch (Throwable e) {
            log.info("Not protobufferable: {}, {}, {}", serializable.getClass().getSimpleName(), storageFile, e.getStackTrace());
        }

        try {
            if (!dir.exists())
                if (!dir.mkdir())
                    log.warn("make dir failed");

            tempFile = File.createTempFile("temp", null, dir);
            tempFile.deleteOnExit();
            if (serializable instanceof PlainTextWrapper) {
                // When we dump json files we don't want to safe it as java serialized string objects, so we use PrintWriter instead.
                printWriter = new PrintWriter(tempFile);
                printWriter.println(((PlainTextWrapper) serializable).plainText);
            } else if (message != null) {
                fileOutputStream = new FileOutputStream(tempFile);

                log.info("Writing protobuffer to disc");
                message.writeTo(fileOutputStream);

                // Attempt to force the bits to hit the disk. In reality the OS or hard disk itself may still decide
                // to not write through to physical media for at least a few seconds, but this is the best we can do.
                fileOutputStream.flush();
                fileOutputStream.getFD().sync();

                // Close resources before replacing file with temp file because otherwise it causes problems on windows
                // when rename temp file
                fileOutputStream.close();
            } else {
                // Don't use auto closeable resources in try() as we would need too many try/catch clauses (for tempFile)
                // and we need to close it
                // manually before replacing file with temp file
                fileOutputStream = new FileOutputStream(tempFile);
                objectOutputStream = new ObjectOutputStream(fileOutputStream);

                objectOutputStream.writeObject(serializable);
                // Attempt to force the bits to hit the disk. In reality the OS or hard disk itself may still decide
                // to not write through to physical media for at least a few seconds, but this is the best we can do.
                fileOutputStream.flush();
                fileOutputStream.getFD().sync();

                // Close resources before replacing file with temp file because otherwise it causes problems on windows
                // when rename temp file
                fileOutputStream.close();
                objectOutputStream.close();
            }
            renameTempFileToFile(tempFile, storageFile);
        } catch (Throwable t) {
            log.error("storageFile " + storageFile.toString());
            t.printStackTrace();
            log.error("Error at saveToFile: " + t.getMessage());
        } finally {
            if (tempFile != null && tempFile.exists()) {
                log.warn("Temp file still exists after failed save. We will delete it now. storageFile=" + storageFile);
                if (!tempFile.delete())
                    log.error("Cannot delete temp file.");
            }

            try {
                if (objectOutputStream != null)
                    objectOutputStream.close();
                if (fileOutputStream != null)
                    fileOutputStream.close();
                if (printWriter != null)
                    printWriter.close();
            } catch (IOException e) {
                // We swallow that
                e.printStackTrace();
                log.error("Cannot close resources." + e.getMessage());
            }
        }
    }

    private synchronized void renameTempFileToFile(File tempFile, File file) throws IOException {
        if (Utilities.isWindows()) {
            // Work around an issue on Windows whereby you can't rename over existing files.
            final File canonical = file.getCanonicalFile();
            if (canonical.exists() && !canonical.delete()) {
                throw new IOException("Failed to delete canonical file for replacement with save");
            }
            if (!tempFile.renameTo(canonical)) {
                throw new IOException("Failed to rename " + tempFile + " to " + canonical);
            }
        } else if (!tempFile.renameTo(file)) {
            throw new IOException("Failed to rename " + tempFile + " to " + file);
        }
    }


    //////////////////////////////// DISK /////////////////////////////////////

    public Optional<Persistable> fromProto(PB.DiskEnvelope envelope) {
        if (Objects.isNull(envelope)) {
            log.warn("fromProtoBuf called with empty disk envelope.");
            return Optional.empty();
        }

        log.debug("Convert protobuffer disk envelope: {}", envelope.getMessageCase());

        Persistable result = null;
        switch (envelope.getMessageCase()) {
            case ADDRESS_ENTRY_LIST:
                addToAddressEntryList(envelope, AddressEntryList );
                result = AddressEntryList ;
                break;
                /*
            case NAVIGATION:
                result = getPing(envelope);
                break;
            case PERSISTED_PEERS:
                result = getPing(envelope);
                break;
                */
            case PREFERENCES:
                setPreferences(envelope, preferences);
                break;
                /*
            case USER:
                result = getPing(envelope);
                break;
            case PERSISTED_P2P_STORAGE_DATA:
                result = getPing(envelope);
                break;
            case SEQUENCE_NUMBER_MAP:
                result = getPing(envelope);
                break;
                */
            default:
                log.warn("Unknown message case:{}:{}", envelope.getMessageCase());
        }
        return Optional.ofNullable(result);
    }

    private void setPreferences(PB.DiskEnvelope envelope, Preferences preferences) {
        preferences.setUserLanguage(envelope.getPreferences().getUserLanguage());
        PB.Country userCountry = envelope.getPreferences().getUserCountry();
        preferences.setUserCountry(new Country(userCountry.getCode(), userCountry.getName(), new Region(userCountry.getRegion().getCode(), userCountry.getRegion().getName())));
        envelope.getPreferences().getFiatCurrenciesList().stream()
                .forEach(tradeCurrency -> preferences.addFiatCurrency((FiatCurrency) getTradeCurrency(tradeCurrency)));
        envelope.getPreferences().getCryptoCurrenciesList().stream()
                .forEach(tradeCurrency -> preferences.addCryptoCurrency((CryptoCurrency) getTradeCurrency(tradeCurrency)));
        PB.BlockChainExplorer bceMain = envelope.getPreferences().getBlockChainExplorerMainNet();
        preferences.setBlockChainExplorerMainNet(new BlockChainExplorer(bceMain.getName(), bceMain.getTxUrl(), bceMain.getAddressUrl()));
        PB.BlockChainExplorer bceTest = envelope.getPreferences().getBlockChainExplorerTestNet();
        preferences.setBlockChainExplorerTestNet(new BlockChainExplorer(bceTest.getName(), bceTest.getTxUrl(), bceTest.getAddressUrl()));
        preferences.setBackupDirectory(envelope.getPreferences().getBackupDirectory());
        preferences.setAutoSelectArbitrators(envelope.getPreferences().getAutoSelectArbitrators());
        preferences.setDontShowAgainMap(envelope.getPreferences().getDontShowAgainMapMap());
        preferences.setTacAccepted(envelope.getPreferences().getTacAccepted());
        preferences.setUseTorForBitcoinJ(envelope.getPreferences().getUseTorForBitcoinJ());
        preferences.setShowOwnOffersInOfferBook(envelope.getPreferences().getShowOwnOffersInOfferBook());
        PB.Locale preferredLocale = envelope.getPreferences().getPreferredLocale();
        PB.TradeCurrency preferredTradeCurrency = envelope.getPreferences().getPreferredTradeCurrency();
        preferences.setPreferredTradeCurrency(getTradeCurrency(preferredTradeCurrency));
        preferences.setWithdrawalTxFeeInBytes(envelope.getPreferences().getWithdrawalTxFeeInBytes());
        preferences.setMaxPriceDistanceInPercent(envelope.getPreferences().getMaxPriceDistanceInPercent());
        preferences.setOfferBookChartScreenCurrencyCode(envelope.getPreferences().getOfferBookChartScreenCurrencyCode());
        preferences.setTradeChartsScreenCurrencyCode(envelope.getPreferences().getTradeChartsScreenCurrencyCode());
        preferences.setUseStickyMarketPrice(envelope.getPreferences().getUseStickyMarketPrice());
        preferences.setSortMarketCurrenciesNumerically(envelope.getPreferences().getSortMarketCurrenciesNumerically());
        preferences.setUsePercentageBasedPrice(envelope.getPreferences().getUsePercentageBasedPrice());
        preferences.setPeerTagMap(envelope.getPreferences().getPeerTagMapMap());
        preferences.setBitcoinNodes(envelope.getPreferences().getBitcoinNodes());
        preferences.setIgnoreTradersList(envelope.getPreferences().getIgnoreTradersListList());
        preferences.setDirectoryChooserPath(envelope.getPreferences().getDirectoryChooserPath());
        preferences.setBuyerSecurityDepositAsLong(envelope.getPreferences().getBuyerSecurityDepositAsLong());
    }

    private TradeCurrency getTradeCurrency(PB.TradeCurrency tradeCurrency) {
        switch (tradeCurrency.getMessageCase()) {
            case FIAT_CURRENCY:
                return new FiatCurrency(tradeCurrency.getCode(), getLocale(tradeCurrency.getFiatCurrency().getDefaultLocale()));
            case CRYPTO_CURRENCY:
                return new CryptoCurrency(tradeCurrency.getCode(), tradeCurrency.getName(), tradeCurrency.getSymbol(),
                        tradeCurrency.getCryptoCurrency().getIsAsset());
            default:
                log.warn("Unknown tradecurrency: {}", tradeCurrency.getMessageCase());
        }

        return null;
    }

    private Locale getLocale(PB.Locale locale) {
        return new Locale(locale.getLanguage(), locale.getCountry(), locale.getVariant());
    }

    private void addToAddressEntryList(PB.DiskEnvelope envelope, AddressEntryList  AddressEntryList ) {
        envelope.getAddressEntryList().getAddressEntryList().stream().map(addressEntry -> AddressEntryList .addAddressEntry(
                new AddressEntry(addressEntry.getPubKey().toByteArray(), addressEntry.getPubKeyHash().toByteArray(), addressEntry.getParamId(), AddressEntry.Context.valueOf(addressEntry.getContext().name()),
                        addressEntry.getOfferId(), Coin.valueOf(addressEntry.getCoinLockedInMultiSig().getValue()), AddressEntryList .getKeyBagSupplier())));
    }

}
