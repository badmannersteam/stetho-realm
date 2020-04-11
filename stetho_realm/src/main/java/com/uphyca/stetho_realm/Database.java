/*
 * Copyright (c) 2015-present, uPhyca, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

package com.uphyca.stetho_realm;

import android.database.sqlite.SQLiteException;

import com.facebook.stetho.common.Util;
import com.facebook.stetho.inspector.jsonrpc.JsonRpcPeer;
import com.facebook.stetho.inspector.jsonrpc.JsonRpcResult;
import com.facebook.stetho.inspector.protocol.ChromeDevtoolsDomain;
import com.facebook.stetho.inspector.protocol.ChromeDevtoolsMethod;
import com.facebook.stetho.json.ObjectMapper;
import com.facebook.stetho.json.annotation.JsonProperty;

import org.json.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import io.realm.RealmFieldType;
import io.realm.internal.OsList;
import io.realm.internal.OsResults;
import io.realm.internal.Row;
import io.realm.internal.Table;


@SuppressWarnings("WeakerAccess")
public class Database implements ChromeDevtoolsDomain {

    private static final String NULL = "[null]";

    private final RealmPeerManager realmPeerManager;
    private final ObjectMapper objectMapper;
    private final boolean withMetaTables;
    private final long limit;
    private final boolean ascendingOrder;

    private DateFormat dateTimeFormatter;

    private enum StethoRealmFieldType {
        INTEGER(0),
        BOOLEAN(1),
        STRING(2),
        BINARY(4),
        UNSUPPORTED_TABLE(5),
        UNSUPPORTED_MIXED(6),
        OLD_DATE(7),
        DATE(8),
        FLOAT(9),
        DOUBLE(10),
        OBJECT(12),
        LIST(13),
        // BACKLINK(14); Not exposed until needed
        INTEGER_LIST(15),
        BOOLEAN_LIST(16),
        STRING_LIST(17),
        BINARY_LIST(18),
        DATE_LIST(19),
        FLOAT_LIST(20),
        DOUBLE_LIST(21),

        // Stetho Realmが勝手に定義した特別な値
        UNKNOWN(-1);

        private final int nativeValue;

        StethoRealmFieldType(int nativeValue) {
            this.nativeValue = nativeValue;
        }

        @SuppressWarnings("unused")
        public int getValue() {
            return nativeValue;
        }
    }

    /**
     * 指定されたパラメータで {@link Database}インスタンスを構築します。
     *
     * @param packageName アプリケーションのパッケージネーム(application ID)。
     * @param filesProvider {@link RealmFilesProvider} インスタンス。
     * @param withMetaTables テーブル一覧にmeta テーブルを含めるかどうか。
     * @param limit 返却するデータの最大行数
     * @param ascendingOrder {@code true}ならデータを id列の昇順に、{@code false}なら降順に返します。
     * @param defaultEncryptionKey データベースの復号に使用するキー。
     * {@code null} の場合は暗号化されていないものとして扱います。
     * また、 {@code encryptionKeys} で個別のキーが指定されている
     * データベースについては {@code encryptionKeys}の指定が優先されます。
     * @param encryptionKeys データベース個別のキーを指定するマップ。
     */
    Database(String packageName,
            RealmFilesProvider filesProvider,
            boolean withMetaTables,
            long limit,
            boolean ascendingOrder,
            byte[] defaultEncryptionKey,
            Map<String, byte[]> encryptionKeys) {
        this.realmPeerManager = new RealmPeerManager(packageName, filesProvider, defaultEncryptionKey, encryptionKeys);
        this.objectMapper = new ObjectMapper();
        this.withMetaTables = withMetaTables;
        this.limit = limit;
        this.ascendingOrder = ascendingOrder;
    }

    @ChromeDevtoolsMethod
    @SuppressWarnings("unused")
    public void enable(JsonRpcPeer peer, JSONObject params) {
        realmPeerManager.addPeer(peer);
    }

    @ChromeDevtoolsMethod
    @SuppressWarnings("unused")
    public void disable(JsonRpcPeer peer, JSONObject params) {
        realmPeerManager.removePeer(peer);
    }

    @ChromeDevtoolsMethod
    @SuppressWarnings("unused")
    public JsonRpcResult getDatabaseTableNames(JsonRpcPeer peer, JSONObject params) {
        GetDatabaseTableNamesRequest request = objectMapper.convertValue(params, GetDatabaseTableNamesRequest.class);
        GetDatabaseTableNamesResponse response = new GetDatabaseTableNamesResponse();
        response.tableNames = realmPeerManager.getDatabaseTableNames(request.databaseId, withMetaTables);
        return response;
    }

    @ChromeDevtoolsMethod
    @SuppressWarnings("unused")
    public JsonRpcResult executeSQL(JsonRpcPeer peer, JSONObject params) {
        ExecuteSQLRequest request = this.objectMapper.convertValue(params, ExecuteSQLRequest.class);

        try {
            return realmPeerManager.executeSQL(request.databaseId, request.query,
                    new RealmPeerManager.ExecuteResultHandler<ExecuteSQLResponse>() {
                        public ExecuteSQLResponse handleRawQuery() throws SQLiteException {
                            ExecuteSQLResponse response = new ExecuteSQLResponse();
                            response.columnNames = Collections.singletonList("success");
                            response.values = Collections.<Object>singletonList("true");
                            return response;
                        }

                        public ExecuteSQLResponse handleSelect(Table table, boolean addRowIndex) throws SQLiteException {
                            ExecuteSQLResponse response = new ExecuteSQLResponse();

                            final ArrayList<String> columnNames = new ArrayList<>();
                            if (addRowIndex) {
                                columnNames.add("<index>");
                            }
                            for (int i = 0; i < table.getColumnCount(); i++) {
                                columnNames.add(table.getColumnName(i));
                            }

                            response.columnNames = columnNames;
                            response.values = flattenRows(table, limit, addRowIndex);
                            return response;
                        }

                        public ExecuteSQLResponse handleInsert(long insertedId) throws SQLiteException {
                            ExecuteSQLResponse response = new ExecuteSQLResponse();
                            response.columnNames = Collections.singletonList("ID of last inserted row");
                            response.values = Collections.<Object>singletonList(insertedId);
                            return response;
                        }

                        public ExecuteSQLResponse handleUpdateDelete(int count) throws SQLiteException {
                            ExecuteSQLResponse response = new ExecuteSQLResponse();
                            response.columnNames = Collections.singletonList("Modified rows");
                            response.values = Collections.<Object>singletonList(count);
                            return response;
                        }
                    });
        } catch (SQLiteException e) {
            Error error = new Error();
            error.code = 0;
            error.message = e.getMessage();
            ExecuteSQLResponse response = new ExecuteSQLResponse();
            response.sqlError = error;
            return response;
        }
    }

    private List<Object> flattenRows(Table table, long limit, boolean addRowIndex) {
        Util.throwIfNot(limit >= 0);
        final List<Object> flatList = new ArrayList<>();
        long numColumns = table.getColumnCount();

        final long tableSize = table.size();
        String[] columnNames = table.getColumnNames();
        OsResults results = OsResults.createFromQuery(table.getSharedRealm(), table.where());
        for (long index = 0; index < limit && index < tableSize; index++) {
            final long row = ascendingOrder ? index : (tableSize - index - 1);
            final RowWrapper rowData = RowWrapper.wrap(results.getUncheckedRow((int) row));
            if (addRowIndex) {
                flatList.add(rowData.row.getObjectKey());
            }
            for (int column = 0; column < numColumns; column++) {
                long key = table.getColumnKey(columnNames[column]);
                switch (rowData.getColumnType(key)) {
                    case INTEGER:
                        if (rowData.isNull(key)) {
                            flatList.add(NULL);
                        } else {
                            flatList.add(rowData.getLong(key));
                        }
                        break;
                    case BOOLEAN:
                        if (rowData.isNull(key)) {
                            flatList.add(NULL);
                        } else {
                            flatList.add(rowData.getBoolean(key));
                        }
                        break;
                    case STRING:
                        if (rowData.isNull(key)) {
                            flatList.add(NULL);
                        } else {
                            flatList.add(rowData.getString(key));
                        }
                        break;
                    case BINARY:
                        if (rowData.isNull(key)) {
                            flatList.add(NULL);
                        } else {
                            flatList.add(rowData.getBinaryByteArray(key));
                        }
                        break;
                    case FLOAT:
                        if (rowData.isNull(key)) {
                            flatList.add(NULL);
                        } else {
                            final float aFloat = rowData.getFloat(key);
                            if (Float.isNaN(aFloat)) {
                                flatList.add("NaN");
                            } else if (aFloat == Float.POSITIVE_INFINITY) {
                                flatList.add("Infinity");
                            } else if (aFloat == Float.NEGATIVE_INFINITY) {
                                flatList.add("-Infinity");
                            } else {
                                flatList.add(aFloat);
                            }
                        }
                        break;
                    case DOUBLE:
                        if (rowData.isNull(key)) {
                            flatList.add(NULL);
                        } else {
                            final double aDouble = rowData.getDouble(key);
                            if (Double.isNaN(aDouble)) {
                                flatList.add("NaN");
                            } else if (aDouble == Double.POSITIVE_INFINITY) {
                                flatList.add("Infinity");
                            } else if (aDouble == Double.NEGATIVE_INFINITY) {
                                flatList.add("-Infinity");
                            } else {
                                flatList.add(aDouble);
                            }
                        }
                        break;
                    case OLD_DATE:
                    case DATE:
                        if (rowData.isNull(key)) {
                            flatList.add(NULL);
                        } else {
                            flatList.add(formatDate(rowData.getDate(key)));
                        }
                        break;
                    case OBJECT:
                        if (rowData.isNullLink(key)) {
                            flatList.add(NULL);
                        } else {
                            flatList.add(rowData.getLink(key));
                        }
                        break;
                    case LIST:
                        // LIST never be null
                        flatList.add(formatList(rowData.getLinkList(key)));
                        break;
                    case INTEGER_LIST:
                    case BOOLEAN_LIST:
                    case DOUBLE_LIST:
                    case STRING_LIST:
                    case BINARY_LIST:
                    case DATE_LIST:
                    case FLOAT_LIST:
                        if (rowData.isNullLink(key)) {
                            flatList.add(NULL);
                        } else {
                            RealmFieldType columnType = table.getColumnType(key);
                            flatList.add(formatValueList(rowData.getValueList(key, columnType), columnType));
                        }
                        break;
                    default:
                        flatList.add("unknown column type: " + rowData.getColumnType(key));
                        break;
                }
            }
        }

        if (limit < table.size()) {
            for (int column = 0; column < numColumns; column++) {
                flatList.add("{truncated}");
            }
        }

        return flatList;
    }

    private String formatValueList(OsList linkList, RealmFieldType columnType) {
        final StringBuilder sb = new StringBuilder(columnType.name());
        sb.append("{");

        final long size = linkList.size();
        for (long pos = 0; pos < size; pos++) {
            sb.append(linkList.getValue(pos));
            sb.append(',');
        }
        if (size != 0) {
            // remove last ','
            sb.setLength(sb.length() - 1);
        }

        sb.append("}");
        return sb.toString();
    }


    private static class GetDatabaseTableNamesRequest {
        @JsonProperty(required = true)
        public String databaseId;
    }

    private static class GetDatabaseTableNamesResponse implements JsonRpcResult {
        @JsonProperty(required = true)
        public List<String> tableNames;
    }

    private static class ExecuteSQLRequest {
        @JsonProperty(required = true)
        public String databaseId;

        @JsonProperty(required = true)
        public String query;
    }

    private static class ExecuteSQLResponse implements JsonRpcResult {
        @JsonProperty
        public List<String> columnNames;

        @JsonProperty
        public List<Object> values;

        @JsonProperty
        public Error sqlError;
    }

    public static class AddDatabaseEvent {
        @JsonProperty(required = true)
        public DatabaseObject database;
    }

    public static class DatabaseObject {
        @JsonProperty(required = true)
        public String id;

        @JsonProperty(required = true)
        public String domain;

        @JsonProperty(required = true)
        public String name;

        @JsonProperty(required = true)
        public String version;
    }

    public static class Error {
        @JsonProperty(required = true)
        public String message;

        @JsonProperty(required = true)
        public int code;
    }

    private String formatDate(Date date) {
        if (dateTimeFormatter == null) {
            dateTimeFormatter = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.LONG, SimpleDateFormat.LONG);
        }
        return dateTimeFormatter.format(date) + " (" + date.getTime() + ')';
    }

    private String formatList(OsList linkList) {
        final StringBuilder sb = new StringBuilder(linkList.getTargetTable().getName());
        sb.append("{");

        final long size = linkList.size();
        for (long pos = 0; pos < size; pos++) {
            sb.append(linkList.getUncheckedRow(pos).getObjectKey());
            sb.append(',');
        }
        if (size != 0) {
            // remove last ','
            sb.setLength(sb.length() - 1);
        }

        sb.append("}");
        return sb.toString();
    }

    static class RowWrapper {
        static RowWrapper wrap(Row row) {
            return new RowWrapper(row);
        }

        private final Row row;

        RowWrapper(Row row) {
            this.row = row;
        }

        StethoRealmFieldType getColumnType(long columnKey) {
            // io.realm.RealmFieldType
            final Enum<?> columnType = row.getColumnType(columnKey);
            final String name = columnType.name();
            if (name.equals("INTEGER")) {
                return StethoRealmFieldType.INTEGER;
            }
            if (name.equals("BOOLEAN")) {
                return StethoRealmFieldType.BOOLEAN;
            }
            if (name.equals("STRING")) {
                return StethoRealmFieldType.STRING;
            }
            if (name.equals("BINARY")) {
                return StethoRealmFieldType.BINARY;
            }
            if (name.equals("UNSUPPORTED_TABLE")) {
                return StethoRealmFieldType.UNSUPPORTED_TABLE;
            }
            if (name.equals("UNSUPPORTED_MIXED")) {
                return StethoRealmFieldType.UNSUPPORTED_MIXED;
            }
            if (name.equals("UNSUPPORTED_DATE")) {
                return StethoRealmFieldType.OLD_DATE;
            }
            if (name.equals("DATE")) {
                return StethoRealmFieldType.DATE;
            }
            if (name.equals("FLOAT")) {
                return StethoRealmFieldType.FLOAT;
            }
            if (name.equals("DOUBLE")) {
                return StethoRealmFieldType.DOUBLE;
            }
            if (name.equals("OBJECT")) {
                return StethoRealmFieldType.OBJECT;
            }
            if (name.equals("LIST")) {
                return StethoRealmFieldType.LIST;
            }
            if (name.equals("INTEGER_LIST")) {
                return StethoRealmFieldType.INTEGER_LIST;
            }
            if (name.equals("BOOLEAN_LIST")) {
                return StethoRealmFieldType.BOOLEAN_LIST;
            }
            if (name.equals("DOUBLE_LIST")) {
                return StethoRealmFieldType.DOUBLE_LIST;
            }
            if (name.equals("STRING_LIST")) {
                return StethoRealmFieldType.STRING_LIST;
            }
            if (name.equals("BINARY_LIST")) {
                return StethoRealmFieldType.BINARY_LIST;
            }
            if (name.equals("DATE_LIST")) {
                return StethoRealmFieldType.DATE_LIST;
            }
            if (name.equals("FLOAT_LIST")) {
                return StethoRealmFieldType.FLOAT_LIST;
            }
            return StethoRealmFieldType.UNKNOWN;
        }

        boolean isNull(long columnKey) {
            return row.isNull(columnKey);
        }

        boolean isNullLink(long columnKey) {
            return row.isNullLink(columnKey);
        }

        long getLong(long columnKey) {
            return row.getLong(columnKey);
        }

        boolean getBoolean(long columnKey) {
            return row.getBoolean(columnKey);
        }

        float getFloat(long columnKey) {
            return row.getFloat(columnKey);
        }

        double getDouble(long columnKey) {
            return row.getDouble(columnKey);
        }

        Date getDate(long columnKey) {
            return row.getDate(columnKey);
        }

        String getString(long columnKey) {
            return row.getString(columnKey);
        }

        byte[] getBinaryByteArray(long columnKey) {
            return row.getBinaryByteArray(columnKey);
        }

        long getLink(long columnKey) {
            return row.getLink(columnKey);
        }

        OsList getLinkList(long columnKey) {
            return row.getModelList(columnKey);
        }

        OsList getValueList(long columnKey, RealmFieldType fieldType) {
            return row.getValueList(columnKey, fieldType);
        }
    }
}
