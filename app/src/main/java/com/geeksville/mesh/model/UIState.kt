package com.geeksville.mesh.model

import android.app.Application
import android.content.Context
import android.content.SharedPreferences
import android.net.Uri
import android.os.RemoteException
import android.view.Menu
import androidx.core.content.edit
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.asLiveData
import androidx.lifecycle.viewModelScope
import com.geeksville.mesh.android.Logging
import com.geeksville.mesh.*
import com.geeksville.mesh.ChannelProtos.ChannelSettings
import com.geeksville.mesh.ConfigProtos.Config
import com.geeksville.mesh.ConfigProtos.Config.LoRaConfig.ModemPreset
import com.geeksville.mesh.database.MeshLogRepository
import com.geeksville.mesh.database.QuickChatActionRepository
import com.geeksville.mesh.database.entity.Packet
import com.geeksville.mesh.database.entity.MeshLog
import com.geeksville.mesh.database.entity.QuickChatAction
import com.geeksville.mesh.LocalOnlyProtos.LocalConfig
import com.geeksville.mesh.LocalOnlyProtos.LocalModuleConfig
import com.geeksville.mesh.database.PacketRepository
import com.geeksville.mesh.repository.datastore.RadioConfigRepository
import com.geeksville.mesh.repository.radio.RadioInterfaceService
import com.geeksville.mesh.service.MeshService
import com.geeksville.mesh.util.positionToMeter
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.mapLatest
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedWriter
import java.io.FileNotFoundException
import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Locale
import javax.inject.Inject
import kotlin.math.roundToInt
import com.geeksville.mesh.Position
import kotlinx.coroutines.flow.toList

/// Given a human name, strip out the first letter of the first three words and return that as the initials for
/// that user. If the original name is only one word, strip vowels from the original name and if the result is
/// 3 or more characters, use the first three characters. If not, just take the first 3 characters of the
/// original name.
fun getInitials(nameIn: String): String {
    val nchars = 4
    val minchars = 2
    val name = nameIn.trim()
    val words = name.split(Regex("\\s+")).filter { it.isNotEmpty() }

    val initials = when (words.size) {
        in 0 until minchars -> {
            val nm = if (name.isNotEmpty())
                name.first() + name.drop(1).filterNot { c -> c.lowercase() in "aeiou" }
            else
                ""
            if (nm.length >= nchars) nm else name
        }
        else -> words.map { it.first() }.joinToString("")
    }
    return initials.take(nchars)
}

/**
 * Builds a [Channel] list from the difference between two [ChannelSettings] lists.
 * Only changes are included in the resulting list.
 *
 * @param new The updated [ChannelSettings] list.
 * @param old The current [ChannelSettings] list (required to disable unused channels).
 * @return A [Channel] list containing only the modified channels.
 */
internal fun getChannelList(
    new: List<ChannelSettings>,
    old: List<ChannelSettings>,
): List<ChannelProtos.Channel> = buildList {
    for (i in 0..maxOf(old.lastIndex, new.lastIndex)) {
        if (old.getOrNull(i) != new.getOrNull(i)) add(channel {
            role = when (i) {
                0 -> ChannelProtos.Channel.Role.PRIMARY
                in 1..new.lastIndex -> ChannelProtos.Channel.Role.SECONDARY
                else -> ChannelProtos.Channel.Role.DISABLED
            }
            index = i
            settings = new.getOrNull(i) ?: channelSettings { }
        })
    }
}

@HiltViewModel
class UIViewModel @Inject constructor(
    private val app: Application,
    val nodeDB: NodeDB,
    private val radioConfigRepository: RadioConfigRepository,
    private val radioInterfaceService: RadioInterfaceService,
    private val meshLogRepository: MeshLogRepository,
    private val packetRepository: PacketRepository,
    private val quickChatActionRepository: QuickChatActionRepository,
    private val preferences: SharedPreferences
) : ViewModel(), Logging {

    var actionBarMenu: Menu? = null
    val meshService: IMeshService? get() = radioConfigRepository.meshService

    val bondedAddress get() = radioInterfaceService.getBondedDeviceAddress()
    val selectedBluetooth get() = radioInterfaceService.getDeviceAddress()?.getOrNull(0) == 'x'

    private val _meshLog = MutableStateFlow<List<MeshLog>>(emptyList())
    val meshLog: StateFlow<List<MeshLog>> = _meshLog

    private val _packets = MutableStateFlow<List<Packet>>(emptyList())
    val packets: StateFlow<List<Packet>> = _packets

    private val _localConfig = MutableStateFlow<LocalConfig>(LocalConfig.getDefaultInstance())
    val localConfig: StateFlow<LocalConfig> = _localConfig
    val config get() = _localConfig.value

    private val _moduleConfig = MutableStateFlow<LocalModuleConfig>(LocalModuleConfig.getDefaultInstance())
    val moduleConfig: StateFlow<LocalModuleConfig> = _moduleConfig
    val module get() = _moduleConfig.value

    private val _channels = MutableStateFlow(channelSet {})
    val channels: StateFlow<AppOnlyProtos.ChannelSet> get() = _channels
    val channelSet get() = channels.value

    private val _quickChatActions = MutableStateFlow<List<QuickChatAction>>(emptyList())
    val quickChatActions: StateFlow<List<QuickChatAction>> = _quickChatActions

    private val _focusedNode = MutableStateFlow<NodeInfo?>(null)
    val focusedNode: StateFlow<NodeInfo?> = _focusedNode

    // hardware info about our local device (can be null)
    val myNodeInfo: StateFlow<MyNodeInfo?> get() = nodeDB.myNodeInfo
    val ourNodeInfo: StateFlow<NodeInfo?> get() = nodeDB.ourNodeInfo

    private val requestIds = MutableStateFlow<HashMap<Int, Boolean>>(hashMapOf())

    private val _snackbarText = MutableLiveData<Any?>(null)
    val snackbarText: LiveData<Any?> get() = _snackbarText

    init {
        radioInterfaceService.errorMessage.filterNotNull().onEach {
            _snackbarText.value = it
            radioInterfaceService.clearErrorMessage()
        }.launchIn(viewModelScope)

        viewModelScope.launch {
            meshLogRepository.getAllLogs().collect { logs ->
                _meshLog.value = logs
            }
        }
        viewModelScope.launch {
            packetRepository.getAllPackets().collect { packets ->
                _packets.value = packets
            }
        }
        radioConfigRepository.localConfigFlow.onEach { config ->
            _localConfig.value = config
        }.launchIn(viewModelScope)
        radioConfigRepository.moduleConfigFlow.onEach { config ->
            _moduleConfig.value = config
        }.launchIn(viewModelScope)
        viewModelScope.launch {
            quickChatActionRepository.getAllActions().collect { actions ->
                _quickChatActions.value = actions
            }
        }
        radioConfigRepository.channelSetFlow.onEach { channelSet ->
            _channels.value = channelSet
        }.launchIn(viewModelScope)

        viewModelScope.launch {
            combine(meshLogRepository.getAllLogs(9), requestIds) { list, ids ->
                val unprocessed = ids.filterValues { !it }.keys.ifEmpty { return@combine emptyList() }
                list.filter { log -> log.meshPacket?.decoded?.requestId in unprocessed }
            }.collect { it.forEach(::processPacketResponse) }
        }
        debug("ViewModel created")
    }

    private val contactKey: MutableStateFlow<String> = MutableStateFlow(DataPacket.ID_BROADCAST)
    fun setContactKey(contact: String) {
        contactKey.value = contact
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    val messages: LiveData<List<Packet>> = contactKey.flatMapLatest { contactKey ->
        packetRepository.getMessagesFrom(contactKey)
    }.asLiveData()

    val contacts = combine(packetRepository.getContacts(), channels) { contacts, channelSet ->
        // Add empty channel placeholders (always show Broadcast contacts, even when empty)
        val placeholder = (0 until channelSet.settingsCount).associate { ch ->
            val contactKey = "$ch${DataPacket.ID_BROADCAST}"
            val data = DataPacket(bytes = null, dataType = 1, time = 0L, channel = ch)
            contactKey to Packet(0L, 1, contactKey, 0L, data)
        }
        contacts + (placeholder - contacts.keys)
    }.asLiveData()

    @OptIn(ExperimentalCoroutinesApi::class)
    val waypoints: LiveData<Map<Int, Packet>> = _packets.mapLatest { list ->
        list.filter { it.port_num == Portnums.PortNum.WAYPOINT_APP_VALUE }
            .associateBy { packet -> packet.data.waypoint!!.id }
            .filterValues { it.data.waypoint!!.expire > System.currentTimeMillis() / 1000 }
    }.asLiveData()

    private val _destNode = MutableStateFlow<NodeInfo?>(null)
    val destNode: StateFlow<NodeInfo?> get() = if (_destNode.value != null) _destNode else ourNodeInfo

    /**
     * Sets the destination [NodeInfo] used in Radio Configuration.
     * @param node Destination [NodeInfo] (or null for our local NodeInfo).
     */
    fun setDestNode(node: NodeInfo?) {
        _destNode.value = node
    }

    fun generatePacketId(): Int? {
        return try {
            meshService?.packetId
        } catch (ex: RemoteException) {
            errormsg("RemoteException: ${ex.message}")
            return null
        }
    }

    fun sendMessage(str: String, contactKey: String = "0${DataPacket.ID_BROADCAST}") {
        // contactKey: unique contact key filter (channel)+(nodeId)
        val channel = contactKey[0].digitToIntOrNull()
        val dest = if (channel != null) contactKey.substring(1) else contactKey

        val p = DataPacket(dest, channel ?: 0, str)
        sendDataPacket(p)
    }

    fun sendWaypoint(wpt: MeshProtos.Waypoint, contactKey: String = "0${DataPacket.ID_BROADCAST}") {
        // contactKey: unique contact key filter (channel)+(nodeId)
        val channel = contactKey[0].digitToIntOrNull()
        val dest = if (channel != null) contactKey.substring(1) else contactKey

        val p = DataPacket(dest, channel ?: 0, wpt)
        if (wpt.id != 0) sendDataPacket(p)
    }

    private fun sendDataPacket(p: DataPacket) {
        try {
            meshService?.send(p)
        } catch (ex: RemoteException) {
            errormsg("Send DataPacket error: ${ex.message}")
        }
    }

    fun requestTraceroute(destNum: Int) {
        try {
            val packetId = meshService?.packetId ?: return
            meshService?.requestTraceroute(packetId, destNum)
            requestIds.update { it.apply { put(packetId, false) } }
        } catch (ex: RemoteException) {
            errormsg("Request traceroute error: ${ex.message}")
        }
    }

    fun requestPosition(destNum: Int, position: Position = Position(0.0, 0.0, 0)) {
        try {
            meshService?.requestPosition(destNum, position)
        } catch (ex: RemoteException) {
            errormsg("Request position error: ${ex.message}")
        }
    }

    fun deleteAllLogs() = viewModelScope.launch(Dispatchers.IO) {
        meshLogRepository.deleteAll()
    }

    fun deleteAllMessages() = viewModelScope.launch(Dispatchers.IO) {
        packetRepository.deleteAllMessages()
    }

    fun deleteMessages(uuidList: List<Long>) = viewModelScope.launch(Dispatchers.IO) {
        packetRepository.deleteMessages(uuidList)
    }

    fun deleteWaypoint(id: Int) = viewModelScope.launch(Dispatchers.IO) {
        packetRepository.deleteWaypoint(id)
    }

    companion object {
        fun getPreferences(context: Context): SharedPreferences =
            context.getSharedPreferences("ui-prefs", Context.MODE_PRIVATE)
    }

    // Connection state to our radio device
    val connectionState get() = radioConfigRepository.connectionState.asLiveData()
    fun isConnected() = connectionState.value != MeshService.ConnectionState.DISCONNECTED

    private val _requestChannelUrl = MutableLiveData<Uri?>(null)
    val requestChannelUrl: LiveData<Uri?> get() = _requestChannelUrl

    fun setRequestChannelUrl(channelUrl: Uri) {
        _requestChannelUrl.value = channelUrl
    }

    /**
     * Called immediately after activity observes requestChannelUrl
     */
    fun clearRequestChannelUrl() {
        _requestChannelUrl.value = null
    }

    fun showSnackbar(resString: Any) {
        _snackbarText.value = resString
    }

    /**
     * Called immediately after activity observes [snackbarText]
     */
    fun clearSnackbarText() {
        _snackbarText.value = null
    }

    var txEnabled: Boolean
        get() = config.lora.txEnabled
        set(value) {
            updateLoraConfig { it.copy { txEnabled = value } }
        }

    var region: Config.LoRaConfig.RegionCode
        get() = config.lora.region
        set(value) {
            updateLoraConfig { it.copy { region = value } }
        }

    var ignoreIncomingList: MutableList<Int>
        get() = config.lora.ignoreIncomingList
        set(value) = updateLoraConfig {
            it.copy {
                ignoreIncoming.clear()
                ignoreIncoming.addAll(value)
            }
        }

    // managed mode disables all access to configuration
    val isManaged: Boolean get() = config.device.isManaged

    val myNodeNum get() = myNodeInfo.value?.myNodeNum
    val maxChannels get() = myNodeInfo.value?.maxChannels ?: 8

    override fun onCleared() {
        super.onCleared()
        debug("ViewModel cleared")
    }

    private inline fun updateLoraConfig(crossinline body: (Config.LoRaConfig) -> Config.LoRaConfig) {
        val data = body(config.lora)
        setConfig(config { lora = data })
    }

    // Set the radio config (also updates our saved copy in preferences)
    fun setConfig(config: Config) {
        meshService?.setConfig(config.toByteArray())
    }

    fun setChannel(channel: ChannelProtos.Channel) {
        meshService?.setChannel(channel.toByteArray())
    }

    // Set the radio config (also updates our saved copy in preferences)
    fun setChannels(channelSet: AppOnlyProtos.ChannelSet) = viewModelScope.launch {
        getChannelList(channelSet.settingsList, channels.value.settingsList).forEach(::setChannel)
        radioConfigRepository.replaceAllSettings(channelSet.settingsList)

        val newConfig = config { lora = channelSet.loraConfig }
        if (config.lora != newConfig.lora) setConfig(newConfig)
    }

    val provideLocation = object : MutableLiveData<Boolean>(preferences.getBoolean("provide-location", false)) {
        override fun setValue(value: Boolean) {
            super.setValue(value)

            preferences.edit {
                this.putBoolean("provide-location", value)
            }
        }
    }

    fun setOwner(user: MeshUser) {
        try {
            // Note: we use ?. here because we might be running in the emulator
            meshService?.setOwner(user)
        } catch (ex: RemoteException) {
            errormsg("Can't set username on device, is device offline? ${ex.message}")
        }
    }

    val adminChannelIndex: Int /** matches [MeshService.adminChannelIndex] **/
        get() = channelSet.settingsList.indexOfFirst { it.name.equals("admin", ignoreCase = true) }
            .coerceAtLeast(0)

    /**
     * Write the persisted packet data out to a CSV file in the specified location.
     */
    fun saveMessagesCSV(uri: Uri) {
        viewModelScope.launch(Dispatchers.Main) {
            // Extract distances to this device from position messages and put (node,SNR,distance) in
            // the file_uri

            // Capture the current node value while we're still on main thread
            val nodes = nodeDB.nodes.value

            val positionToPos: (MeshProtos.Position?) -> Position? = { meshPosition ->
                meshPosition?.let { Position(it) }.takeIf {
                    it?.isValid() == true
                }
            }

            writeToUri(uri) { writer ->
                // Create a map of nodes keyed by their ID
                val nodesById = nodes.values.associateBy { it.num }.toMutableMap()
                val nodePositions = mutableMapOf<Int, MeshProtos.Position?>()

                writer.appendLine(
                        "\"t_date\"," + "\"t_time\"," + "\"t_time_ms\"," + "\"t_id\"," + "\"t_name\"," + "\"t_lat\"," + "\"t_long\"," + "\"t_alt\"," + "\"t_PDOP\"," + "\"t_ground_speed\"," + "\"t_ground_track\"," + "\"t_sats_in_view\"," + "\"t_battery_level\"," + "\"t_voltage\"," + "\"t_channel_utilization\"," + "\"t_air_util_tx\"," +
                        "\"r_date\"," + "\"r_time\"," + "\"r_time_ms\"," + "\"r_id\"," + "\"r_name\"," + "\"r_lat\"," + "\"r_long\"," + "\"r_alt\"," + "\"r_RSSI\"," + "\"r_SNR\"," +
                        "\"actual_airtime\"," + "\"theoretical_airtime\"," + "\"distance\"," + "\"portnum\"," + "\"decoded_payload\"," + "\"payload\"," + "\"message_serialized_size\"," + "\"message_via_mqtt\"," + "\"message_want_ack\"," +
                        "\"lora_region\"," + "\"lora_region_value\"," + "\"lora_tx_enabled\"," + "\"lora_tx_power\"," + "\"lora_bandwidth\"," + "\"lora_coding_rate\"," + "\"lora_frequency_offset\"," + "\"lora_hop_limit\"," + "\"lora_ignore_mqtt\"," + "\"lora_use_preset\"," + "\"lora_modem_preset_name\"," + "\"lora_modem_preset_value\"," + "\"lora_ignore_incoming_count\"," + "\"lora_ignore_incoming_list\"," + "\"lora_override_duty_cycle\"," + "\"lora_override_frequency\"," + "\"lora_radio_frequency\"," + "\"lora_spread factor\"," + "\"lora_sx126xrx_boosted_gain\"")

                val dateFormat = SimpleDateFormat("\"yyyy-MM-dd\",\"HH:mm:ss\"", Locale.getDefault())
                meshLogRepository.getAllLogsInReceiveOrder(Int.MAX_VALUE).first().forEach { packet ->
                    // If we get a NodeInfo packet, use it to update our position data (if valid)
                    packet.nodeInfo?.let { nodeInfo ->
                        positionToPos.invoke(nodeInfo.position)?.let {
                            nodePositions[nodeInfo.num] = nodeInfo.position
                        }
                    }
                    packet.meshPacket?.let { proto ->
                        // If the packet contains position data then use it to update, if valid
                        packet.position?.let { position ->
                            positionToPos.invoke(position)?.let {
                                nodePositions[(proto.from.takeIf { it != 0 } ?: myNodeNum) as Int] = position
                            }

                        }

                        // Filter out of our results any packet that doesn't report SNR.  This
                        // is primarily ADMIN_APP.
                        if (proto.decoded.portnumValue in setOf(Portnums.PortNum.TEXT_MESSAGE_APP_VALUE,
                                                                Portnums.PortNum.RANGE_TEST_APP_VALUE,
                                                                Portnums.PortNum.POSITION_APP_VALUE,
                                                                Portnums.PortNum.TELEMETRY_APP_VALUE,
                                                                Portnums.PortNum.NODEINFO_APP_VALUE)) {
                            val tDateTime = dateFormat.format(proto.txTime.toLong() * 1000)
                            val tTimeMs = proto.txTimeMs
                            val tId = proto.from.toUInt()
                            val tName = nodesById[proto.from]?.user?.longName ?: ""
                            val tPosition = nodePositions[proto.from]
                            val tPos = positionToPos.invoke(tPosition)
                            val tNode = nodesById[proto.from]
                            val tLat = tPos?.latitude ?: ""
                            val tLong = tPos?.longitude ?: ""
                            val tAlt = tPos?.altitude ?: ""
                            val tPDOP = tPosition?.pdop ?: ""
                            val tGroudSpeed = tPosition?.groundSpeed ?: ""
                            val tGroundTrack = tPosition?.groundTrack ?: ""
                            val tSatsInView = tPosition?.satsInView ?: ""
                            val tBatteryLevel = tNode?.deviceMetrics?.batteryLevel ?: ""
                            val tVoltage = tNode?.deviceMetrics?.voltage ?: ""
                            val tChannelUtilization = tNode?.deviceMetrics?.channelUtilization ?: ""
                            val tAirUtilTx = tNode?.deviceMetrics?.airUtilTx ?: ""

                            val rDateTime = dateFormat.format(packet.received_date)
                            val rTimeMs = proto.rxTimeMs
                            val rId = myNodeNum
                            val rName = nodesById[myNodeNum]?.user?.longName ?: ""
                            val rPosition = nodePositions[myNodeNum]
                            val rPos = positionToPos.invoke(nodePositions[myNodeNum])
                            val rLat = rPos?.latitude ?: ""
                            val rLong = rPos?.longitude ?: ""
                            val rAlt = rPos?.altitude ?: ""
                            val rRSSI = "%d".format(proto.rxRssi)
                            val rSNR = "%f".format(proto.rxSnr)

                            val actual_airtime = proto.actualAirtime
                            val theoretical_airtime = proto.theoreticalAirtime

                            val distance = if (tPos == null || rPos == null) {
                                ""
                            } else {
                                positionToMeter(
                                    rPosition!!, // Use rxPosition but only if rxPos was valid
                                    tPosition!! // Use senderPosition but only if senderPos was valid
                                ).roundToInt().toString()
                            }
                            val portnum = proto.decoded.portnum
                            val decoded_payload = proto.decoded.payload.toString().replace("\"", "\"\"")
                            val payload = when {
                                proto.decoded.portnumValue in setOf(
                                    Portnums.PortNum.TEXT_MESSAGE_APP_VALUE,
                                    Portnums.PortNum.RANGE_TEST_APP_VALUE,
                                ) -> proto.decoded.payload.toStringUtf8()
                                    .replace("\"", "\"\"")
                                (proto.decoded.portnumValue == Portnums.PortNum.POSITION_APP_VALUE)
                                    -> MeshProtos.Position.parseFrom(proto.decoded.payload).toString()
                                    .replace("\"", "\"\"")
                                (proto.decoded.portnumValue == Portnums.PortNum.TELEMETRY_APP_VALUE)
                                    -> TelemetryProtos.Telemetry.parseFrom(proto.decoded.payload).toString()
                                    .replace("\"", "\"\"")
                                (proto.decoded.portnumValue == Portnums.PortNum.NODEINFO_APP_VALUE)
                                    -> MeshProtos.User.parseFrom(proto.decoded.payload).toString()
                                    .replace("\"", "\"\"")
                                else -> ""
                            }
                            val messageSerializedSize = proto.serializedSize
                            val messageViaMqtt = proto.viaMqtt
                            val messageWantAck = proto.wantAck

                            val LoRaRegion = _channels.value.loraConfig.region
                            val LoRaRegionValue = _channels.value.loraConfig.regionValue
                            val LoRaTXEnabled = _channels.value.loraConfig.txEnabled
                            val LoRaTXPower = _channels.value.loraConfig.txPower
                            val LoRaBandwith = _channels.value.loraConfig.bandwidth()
                            val LoRaCodingRate = _channels.value.loraConfig.codingRate()
                            val LoRaFrequencyOffset = _channels.value.loraConfig.frequencyOffset
                            val LoRaHopLimit = _channels.value.loraConfig.hopLimit
                            val LoRaIgnoreMQTT = _channels.value.loraConfig.ignoreMqtt
                            val LoRaUsePreset = _channels.value.loraConfig.usePreset
                            val LoRaModemPresetName = _channels.value.loraConfig.modemPreset.name
                            val LoRaModemPresetValue = _channels.value.loraConfig.modemPresetValue
                            val LoRaIgnoreIncomingCount = _channels.value.loraConfig.ignoreIncomingCount
                            val LoRaIgnoreIncomingList = _channels.value.loraConfig.ignoreIncomingList
                            val LoRaOverrideDutyCycle = _channels.value.loraConfig.overrideDutyCycle
                            val LoRaOverrideFrequency = _channels.value.loraConfig.overrideFrequency
                            val LoRaRadioFrequency = _channels.value.loraConfig.radioFreq(proto.channel)
                            val LoRaSpreadFactor = _channels.value.loraConfig.spreadFactor()
                            val LoRaSX126XRxBosstedGain = _channels.value.loraConfig.sx126XRxBoostedGain
                            
                            writer.appendLine(
                                    "$tDateTime," + "$tTimeMs," + "\"$tId\"," + "\"$tName\"," + "\"$tLat\"," + "\"$tLong\"," + "\"$tAlt\"," + "\"$tPDOP\"," + "\"$tGroudSpeed\"," + "\"$tGroundTrack\"," + "\"$tSatsInView\"," + "\"$tBatteryLevel\"," + "\"$tVoltage\"," + "\"$tChannelUtilization\"," + "\"$tAirUtilTx\"," +
                                    "$rDateTime," + "$rTimeMs," +"\"$rId\"," + "\"$rName\"," + "\"$rLat\"," + "\"$rLong\"," + "\"$rAlt\"," + "\"$rRSSI\"," + "\"$rSNR\"," +
                                    "\"$actual_airtime\"," + "\"$theoretical_airtime\"," + "\"$distance\"," + "\"$portnum\"," + "\"$decoded_payload\"," + "\"$payload\"," + "\"$messageSerializedSize\"," + "\"$messageViaMqtt\"," + "\"$messageWantAck\"," +
                                    "\"$LoRaRegion\"," + "\"$LoRaRegionValue\"," + "\"$LoRaTXEnabled\"," + "\"$LoRaTXPower\"," + "\"$LoRaBandwith\"," + "\"$LoRaCodingRate\"," + "\"$LoRaFrequencyOffset\"," + "\"$LoRaHopLimit\"," + "\"$LoRaIgnoreMQTT\"," + "\"$LoRaUsePreset\"," + "\"$LoRaModemPresetName\"," + "\"$LoRaModemPresetValue\"," + "\"$LoRaIgnoreIncomingCount\"," + "\"$LoRaIgnoreIncomingList\"," + "\"$LoRaOverrideDutyCycle\"," + "\"$LoRaOverrideFrequency\"," + "\"$LoRaRadioFrequency\"," + "\"$LoRaSpreadFactor\"," + "\"$LoRaSX126XRxBosstedGain\"")
                        }
                    }
                }
            }
        }
    }

    private suspend inline fun writeToUri(uri: Uri, crossinline block: suspend (BufferedWriter) -> Unit) {
        withContext(Dispatchers.IO) {
            try {
                app.contentResolver.openFileDescriptor(uri, "wt")?.use { parcelFileDescriptor ->
                    FileWriter(parcelFileDescriptor.fileDescriptor).use { fileWriter ->
                        BufferedWriter(fileWriter).use { writer ->
                            block.invoke(writer)
                        }
                    }
                }
            } catch (ex: FileNotFoundException) {
                errormsg("Can't write file error: ${ex.message}")
            }
        }
    }

    fun addQuickChatAction(name: String, value: String, mode: QuickChatAction.Mode) {
        viewModelScope.launch(Dispatchers.Main) {
            val action = QuickChatAction(0, name, value, mode, _quickChatActions.value.size)
            quickChatActionRepository.insert(action)
        }
    }

    fun deleteQuickChatAction(action: QuickChatAction) {
        viewModelScope.launch(Dispatchers.Main) {
            quickChatActionRepository.delete(action)
        }
    }

    fun updateQuickChatAction(
        action: QuickChatAction,
        name: String?,
        message: String?,
        mode: QuickChatAction.Mode?
    ) {
        viewModelScope.launch(Dispatchers.Main) {
            val newAction = QuickChatAction(
                action.uuid,
                name ?: action.name,
                message ?: action.message,
                mode ?: action.mode,
                action.position
            )
            quickChatActionRepository.update(newAction)
        }
    }

    fun updateActionPositions(actions: List<QuickChatAction>) {
        viewModelScope.launch(Dispatchers.Main) {
            for (position in actions.indices) {
                quickChatActionRepository.setItemPosition(actions[position].uuid, position)
            }
        }
    }

    private val _tracerouteResponse = MutableLiveData<String?>(null)
    val tracerouteResponse: LiveData<String?> get() = _tracerouteResponse

    fun clearTracerouteResponse() {
        _tracerouteResponse.value = null
    }

    private fun processPacketResponse(log: MeshLog?) {
        val packet = log?.meshPacket ?: return
        val data = packet.decoded

        if (data?.portnumValue == Portnums.PortNum.TRACEROUTE_APP_VALUE) {
            val parsed = MeshProtos.RouteDiscovery.parseFrom(data.payload)
            fun nodeName(num: Int) = nodeDB.nodesByNum[num]?.user?.longName
                ?: app.getString(R.string.unknown_username)

            _tracerouteResponse.value = buildString {
                append("${nodeName(packet.to)} --> ")
                parsed.routeList.forEach { num -> append("${nodeName(num)} --> ") }
                append(nodeName(packet.from))
            }
            requestIds.update { it.apply { put(data.requestId, true) } }
        }
    }

    private val _currentTab = MutableLiveData(0)
    val currentTab: LiveData<Int> get() = _currentTab

    fun setCurrentTab(tab: Int) {
        _currentTab.value = tab
    }

    fun focusUserNode(node: NodeInfo?) {
        _currentTab.value = 1
        _focusedNode.value = node
    }
}
