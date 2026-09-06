// ==========================================================================
// IMPORTS
// ==========================================================================
import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:equatable/equatable.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

// ==========================================================================
// 1. ESTADO DE LA CONEXIÓN
// ==========================================================================

/// Represents the status of the WebSocket connection.  ConnectivityServiceStatus
enum ConnectivityServiceStatus {
  /// Successfully connected and responding to pings.
  connected,

  /// Disconnected from the WebSocket server.
  disconnected,

  /// Connected, but the server response time exceeds the unstable threshold.
  unstable,
}

// ==========================================================================
// 2. CONFIGURACIÓN
// ==========================================================================

/// Configuration options for the WebSocket connection checker. ConnectivityServiceConfig
class ConnectivityServiceConfig extends Equatable {
  const ConnectivityServiceConfig({
    this.wsUrl = 'wss://internet-check.quilcoflores.workers.dev',
    this.pingInterval = const Duration(seconds: 15),
    this.pongTimeout = const Duration(seconds: 5),
    this.unstableThreshold = const Duration(seconds: 2),
    this.reconnectDelay = const Duration(seconds: 5),
  });

  /// The WebSocket URL to connect to.
  final String wsUrl;

  /// Interval between consecutive heartbeat pings.
  final Duration pingInterval;

  /// Maximum time to wait for a pong response before considering it a timeout.
  final Duration pongTimeout;

  /// Threshold duration that defines a connection as "unstable".
  final Duration unstableThreshold;

  /// Delay before attempting to reconnect after a disconnection.
  final Duration reconnectDelay;

  @override
  List<Object?> get props => [
    wsUrl,
    pingInterval,
    pongTimeout,
    unstableThreshold,
    reconnectDelay,
  ];

  @override
  String toString() {
    return 'ConnectivityServiceConfig(wsUrl: $wsUrl, pingInterval: $pingInterval, '
        'pongTimeout: $pongTimeout, unstableThreshold: $unstableThreshold)';
  }
}

// ==========================================================================
// 3. SERVICIO PRINCIPAL (SINGLETON)
// ==========================================================================

/// A utility class that checks the status of a WebSocket internet connection.
class ConnectivityService {
  /// Creates an instance. Visible primarily for testing or custom DI.
  ConnectivityService.createInstance({
    ConnectivityServiceConfig? config,
    Connectivity? connectivity,
    StreamController<ConnectivityServiceStatus>? statusController,
  }) : _config = config ?? const ConnectivityServiceConfig(),
       _connectivity = connectivity ?? Connectivity(),
       _statusController =
           statusController ??
           StreamController<ConnectivityServiceStatus>.broadcast() {
    _statusController
      ..onListen = _startMonitoring
      ..onCancel = _stopMonitoring;
  }

  /// Singleton instance.
  static ConnectivityService? _instance;

  /// Access the singleton instance. Creates a new one if disposed.
  static ConnectivityService get instance {
    if (_instance == null || _instance!._isDisposed) {
      _instance = ConnectivityService.createInstance();
    }
    return _instance!;
  }

  /// Short form to access the instance.
  static ConnectivityService get I => instance;

  // ==========================================================================
  // INTERNAL STATE
  // ==========================================================================

  final ConnectivityServiceConfig _config;
  final Connectivity _connectivity;
  final StreamController<ConnectivityServiceStatus> _statusController;

  WebSocketChannel? _channel;
  StreamSubscription? _channelSubscription;
  StreamSubscription<List<ConnectivityResult>>? _connectivitySubscription;
  Timer? _heartbeatTimer;
  Timer? _pongTimeoutTimer;
  Timer? _unstableTimer;
  Timer? _reconnectTimer;

  ConnectivityServiceStatus? _lastStatus;
  bool _isConnecting = false;
  bool _waitingForPong = false;
  final bool _isAppInForeground = true;
  bool _hasNetworkInterface = true;
  bool _isDisposed = false;
  Completer<void>? _verificationCompleter;

  // ==========================================================================
  // PUBLIC API
  // ==========================================================================

  /// A stream that emits the current connection status.
  Stream<ConnectivityServiceStatus> get onStatusChange =>
      _statusController.stream;

  /// Indicates whether there are any active listeners.
  bool get hasListeners => _statusController.hasListener;

  /// Gets the current configuration.
  ConnectivityServiceConfig get config => _config;

  /// Checks if there is an active and stable WebSocket connection.
  Future<bool> get hasConnection async {
    final status = await connectionStatus;
    return status == ConnectivityServiceStatus.connected;
  }

  /// Gets the current WebSocket connection status.
  Future<ConnectivityServiceStatus> get connectionStatus async {
    if (_lastStatus != null) return _lastStatus!;

    if (!hasListeners) {
      await _maybeEmitStatusUpdate();
    }
    return _lastStatus ?? ConnectivityServiceStatus.disconnected;
  }

  // ==========================================================================
  // WEBSOCKET LOGIC
  // ==========================================================================

  Future<void> _connectWebSocket() async {
    if (_isDisposed) return;
    if (_isConnecting || _channel != null) return;
    if (!_isAppInForeground || !_hasNetworkInterface) {
      if (_lastStatus != ConnectivityServiceStatus.disconnected) {
        _emitStatus(ConnectivityServiceStatus.disconnected);
      }
      return;
    }

    _isConnecting = true;

    if (_lastStatus != ConnectivityServiceStatus.disconnected) {
      _emitStatus(ConnectivityServiceStatus.disconnected);
    }

    try {
      final channel = WebSocketChannel.connect(Uri.parse(_config.wsUrl));
      _channel = channel;

      await channel.ready.timeout(
        const Duration(seconds: 5),
        onTimeout: () => throw TimeoutException('WebSocket connection timeout'),
      );

      _channelSubscription = channel.stream.listen(
        _onWebSocketMessage,
        onError: _onWebSocketError,
        onDone: _onWebSocketDone,
        cancelOnError: true,
      );

      final verified = await _verifyConnection(channel);
      if (!verified) throw Exception('No pong received during verification');

      _emitStatus(ConnectivityServiceStatus.connected);
      _startHeartbeat();
    } catch (e) {
      if (kDebugMode) debugPrint('❌ WebSocket Error: $e');
      _handleDisconnect();
    } finally {
      _isConnecting = false;
    }
  }

  Future<bool> _verifyConnection(WebSocketChannel channel) async {
    _waitingForPong = true;
    final completer = Completer<void>();
    _verificationCompleter = completer;

    try {
      channel.sink.add('ping');

      _unstableTimer?.cancel();
      _unstableTimer = Timer(_config.unstableThreshold, () {
        if (_waitingForPong) {
          _emitStatus(ConnectivityServiceStatus.unstable);
        }
      });

      await completer.future.timeout(
        _config.pongTimeout,
        onTimeout: () => throw TimeoutException('Pong timeout'),
      );

      _unstableTimer?.cancel();
      _emitStatus(ConnectivityServiceStatus.connected);
      return true;
    } catch (e) {
      return false;
    } finally {
      _waitingForPong = false;

      _unstableTimer?.cancel();
      _pongTimeoutTimer?.cancel();
      _unstableTimer = null;
      _pongTimeoutTimer = null;

      if (identical(_verificationCompleter, completer)) {
        _verificationCompleter = null;
      }
    }
  }

  void _onWebSocketMessage(dynamic message) {
    if (_isDisposed) return;

    final cleanValue = message
        .toString()
        .trim()
        .replaceAll('"', '')
        .replaceAll("'", '')
        .toLowerCase();
    if (cleanValue != 'pong') return;

    _waitingForPong = false;
    _pongTimeoutTimer?.cancel();
    _unstableTimer?.cancel();

    if (_lastStatus == ConnectivityServiceStatus.unstable) {
      _emitStatus(ConnectivityServiceStatus.connected);
    }

    _verificationCompleter?.complete();
  }

  void _onWebSocketError(Object error) {
    if (_isDisposed) return;

    _verificationCompleter?.completeError(error);
    _handleDisconnect();
  }

  void _onWebSocketDone() {
    if (_isDisposed) return;

    _verificationCompleter?.completeError(Exception('WebSocket closed'));
    _handleDisconnect();
  }

  void _startHeartbeat() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = Timer.periodic(_config.pingInterval, (_) {
      if (_isDisposed || _channel == null || _waitingForPong) return;
      _sendPing();
    });
  }

  void _sendPing() {
    if (_isDisposed) return;

    try {
      _waitingForPong = true;
      _channel!.sink.add('ping');

      _unstableTimer?.cancel();
      _unstableTimer = Timer(_config.unstableThreshold, () {
        if (_waitingForPong && !_isDisposed) {
          _emitStatus(ConnectivityServiceStatus.unstable);
        }
      });

      _pongTimeoutTimer?.cancel();
      _pongTimeoutTimer = Timer(_config.pongTimeout, () {
        if (_waitingForPong && !_isDisposed) {
          _waitingForPong = false;
          _unstableTimer?.cancel();
          _handleDisconnect();
        }
      });
    } catch (e) {
      _waitingForPong = false;
      _handleDisconnect();
    }
  }

  void _handleDisconnect() {
    if (_isDisposed) return;

    _channelSubscription?.cancel();
    _channelSubscription = null;

    _heartbeatTimer?.cancel();
    _pongTimeoutTimer?.cancel();
    _unstableTimer?.cancel();
    _waitingForPong = false;

    final channel = _channel;
    _channel = null;
    unawaited(channel?.sink.close());

    _emitStatus(ConnectivityServiceStatus.disconnected);
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    if (_isDisposed) return; // ✅ FIX #5: No reconectar si está disposed
    if (_reconnectTimer?.isActive ?? false) return;

    // ✅ FIX #3: No agendar reconexión si no hay interfaz de red
    if (!_hasNetworkInterface) {
      if (kDebugMode) debugPrint('️ Reconexión pausada: sin interfaz de red');
      return;
    }

    _reconnectTimer = Timer(_config.reconnectDelay, () {
      _reconnectTimer = null;
      if (hasListeners && !_isDisposed) {
        unawaited(_connectWebSocket());
      }
    });
  }

  // ==========================================================================
  // MONITORING & CONNECTIVITY PLUS
  // ==========================================================================

  void _startMonitoring() {
    if (_isDisposed) return; // ✅ FIX #5: No iniciar si está disposed

    _connectivitySubscription = _connectivity.onConnectivityChanged.listen((
      results,
    ) {
      if (_isDisposed) return; // ✅ FIX #5: Ignorar si está disposed

      final hasNone = results.contains(ConnectivityResult.none);
      _hasNetworkInterface = !hasNone;

      if (hasNone) {
        _emitStatus(ConnectivityServiceStatus.disconnected);
        _reconnectTimer?.cancel();
        _reconnectTimer = null;
        _handleDisconnect();
      } else {
        if (_channel == null && !_isConnecting) {
          unawaited(_connectWebSocket());
        }
      }
    });

    _maybeEmitStatusUpdate();
  }

  void _stopMonitoring() {
    _connectivitySubscription?.cancel();
    _connectivitySubscription = null;

    _channelSubscription?.cancel();
    _channelSubscription = null;

    _heartbeatTimer?.cancel();
    _heartbeatTimer = null;

    _pongTimeoutTimer?.cancel();
    _pongTimeoutTimer = null;

    _unstableTimer?.cancel();
    _unstableTimer = null;

    _reconnectTimer?.cancel();
    _reconnectTimer = null;

    final channel = _channel;
    _channel = null;
    unawaited(channel?.sink.close());

    _lastStatus = null;
  }

  Future<void> _maybeEmitStatusUpdate() async {
    if (_isDisposed) return; // ✅ FIX #5: No actualizar si está disposed
    if (_channel == null && !_isConnecting) {
      await _connectWebSocket();
    }
  }

  void _emitStatus(ConnectivityServiceStatus newStatus) {
    if (_isDisposed) return;

    if (_lastStatus != newStatus) {
      _lastStatus = newStatus;
      if (hasListeners && !_statusController.isClosed) {
        _statusController.add(newStatus);
      }
    }
  }

  /// Disposes of the singleton instance and cleans up resources.

  void dispose() {
    if (_isDisposed) return;

    _isDisposed = true;
    _stopMonitoring();

    // El controller se recreará automáticamente cuando se acceda a instance nuevamente
    _statusController.close();
  }

  /*static Widget streamBuilder({
    Key? key,
    required Widget Function(
      BuildContext context,
      ConnectivityServiceStatus status,
    )
    builder,
    Widget? loadingWidget,
  }) {
    return StreamBuilder<ConnectivityServiceStatus>(
      key: key,
      stream: instance.onStatusChange,
      initialData: ConnectivityServiceStatus.disconnected,
      builder: (context, snapshot) {
        // Mientras espera el primer dato, muestra un widget de carga o nada
        if (snapshot.connectionState == ConnectionState.waiting &&
            snapshot.data == null) {
          return loadingWidget ?? const SizedBox.shrink();
        }

        // Entregamos el estado limpio, sin envoltorios de AsyncSnapshot
        final status = snapshot.data ?? ConnectivityServiceStatus.disconnected;
        return builder(context, status);
      },
    );
  }*/

  static Widget streamBuilder({
    Key? key,
    required Widget Function(
      BuildContext context,
      ConnectivityStatusInfo info, // <-- Aquí está el cambio clave
    )
    builder,
    Widget? loadingWidget,
  }) {
    return StreamBuilder<ConnectivityServiceStatus>(
      key: key,
      stream: instance.onStatusChange,
      initialData: ConnectivityServiceStatus.disconnected,
      builder: (context, snapshot) {
        // Mientras espera el primer dato, muestra un widget de carga o nada
        if (snapshot.connectionState == ConnectionState.waiting &&
            snapshot.data == null) {
          return loadingWidget ?? const SizedBox.shrink();
        }

        // 1. Obtenemos el estado crudo
        final status = snapshot.data ?? ConnectivityServiceStatus.disconnected;

        // 2. Lo convertimos automáticamente en la información completa (Estado + Texto)
        final info = ConnectivityStatusInfo.from(status);

        // 3. Entregamos la información limpia al builder
        return builder(context, info);
      },
    );
  }
}

class ConnectivityStatusInfo {
  final ConnectivityServiceStatus status;
  final String text;

  const ConnectivityStatusInfo({required this.status, required this.text});

  /// Fábrica que traduce automáticamente el enum a texto legible.
  factory ConnectivityStatusInfo.from(ConnectivityServiceStatus status) {
    final String text;
    switch (status) {
      case ConnectivityServiceStatus.connected:
        text = 'Conectado';
        break;
      case ConnectivityServiceStatus.unstable:
        text = 'Inestable';
        break;
      case ConnectivityServiceStatus.disconnected:
        text = 'Desconectado';
        break;
    }
    return ConnectivityStatusInfo(status: status, text: text);
  }
}
