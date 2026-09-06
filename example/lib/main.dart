import 'package:connectivity_service/connectivity_service.dart';
import 'package:flutter/material.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'Connectivity Singleton',
      theme: ThemeData(primarySwatch: Colors.blue),
      home: const ConnectionStatusWidget(),
    );
  }
}

class ConnectionStatusWidget extends StatefulWidget {
  const ConnectionStatusWidget({super.key});

  @override
  State<ConnectionStatusWidget> createState() => _ConnectionStatusWidgetState();
}

class _ConnectionStatusWidgetState extends State<ConnectionStatusWidget> {
  // Instancia del servicio
  final checker = ConnectivityService.instance;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Prueba de Conexión')),
      body: Center(
        child: StreamBuilder<ConnectivityServiceStatus>(
          stream: checker.onStatusChange,
          initialData: ConnectivityServiceStatus.disconnected,
          builder: (context, snapshot) {
            final status =
                snapshot.data ?? ConnectivityServiceStatus.disconnected;

            return Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                // 1. Indicador visual del estado (tu código original)
                if (status == ConnectivityServiceStatus.connected)
                  _buildStatusUI(
                    icon: Icons.wifi,
                    color: Colors.green,
                    title: 'Conexión Estable',
                    subtitle: 'WebSocket conectado y respondiendo',
                  )
                else if (status == ConnectivityServiceStatus.unstable)
                  _buildStatusUI(
                    icon: Icons.wifi_find,
                    color: Colors.orange,
                    title: 'Conexión Inestable',
                    subtitle: 'La respuesta del servidor está tardando',
                  )
                else
                  _buildStatusUI(
                    icon: Icons.wifi_off,
                    color: Colors.red,
                    title: 'Sin Conexión',
                    subtitle: 'Intentando reconectar...',
                  ),

                const SizedBox(height: 40),

                // 2. Botón que requiere internet
                ElevatedButton.icon(
                  onPressed: _handleSendDataAction, // <-- Lógica aquí
                  icon: const Icon(Icons.cloud_upload),
                  label: const Text('Enviar Datos al Servidor'),
                  style: ElevatedButton.styleFrom(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 24,
                      vertical: 16,
                    ),
                    textStyle: const TextStyle(
                      fontSize: 16,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ),
              ],
            );
          },
        ),
      ),
    );
  }

  // ==========================================================================
  // LÓGICA DEL BOTÓN (Verificación Imperativa + Dialog)
  // ==========================================================================
  Future<void> _handleSendDataAction() async {
    // 1. Verificamos el estado de forma imperativa en el momento del clic
    final status = await checker.connectionStatus;

    // 2. Si está desconectado, mostramos el diálogo y detenemos la ejecución
    if (status == ConnectivityServiceStatus.disconnected) {
      if (!mounted) return;

      showDialog(
        context: context,
        builder: (dialogContext) => AlertDialog(
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(16),
          ),
          title: const Row(
            children: [
              Icon(Icons.wifi_off, color: Colors.red, size: 28),
              SizedBox(width: 12),
              Text('Sin Conexión'),
            ],
          ),
          content: const Text(
            'Necesitas una conexión a internet estable para enviar estos datos. '
            'Por favor, revisa tu red e inténtalo de nuevo.',
          ),
          actions: [
            TextButton(
              onPressed: Navigator.of(dialogContext).pop,
              child: const Text(
                'Entendido',
                style: TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
          ],
        ),
      );
      return; // ⛔ DETENEMOS la ejecución aquí. No enviamos nada.
    }

    // 3. (Opcional) Si está inestable, podemos advertir pero dejar continuar,
    // o también bloquear. Depende de tu lógica de negocio.
    if (status == ConnectivityServiceStatus.unstable) {
      if (!mounted) return;
      final shouldContinue = await showDialog<bool>(
        context: context,
        builder: (dialogContext) => AlertDialog(
          title: const Row(
            children: [
              Icon(Icons.warning_amber_rounded, color: Colors.orange, size: 28),
              SizedBox(width: 12),
              Text('Conexión Lenta'),
            ],
          ),
          content: const Text(
            'Tu conexión es inestable en este momento. ¿Deseas intentar enviar los datos de todos modos?',
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(false),
              child: const Text('Cancelar'),
            ),
            ElevatedButton(
              onPressed: () => Navigator.of(dialogContext).pop(true),
              child: const Text('Intentar'),
            ),
          ],
        ),
      );

      // Si el usuario cancela o cierra el diálogo, no hacemos nada
      if (shouldContinue != true) return;
    }

    // 4. Si llegamos aquí, TENEMOS conexión (connected). Procedemos a enviar.
    await _simulateDataSending();
  }

  // Simulación de la tarea que requiere internet
  Future<void> _simulateDataSending() async {
    if (!mounted) return;

    // Mostramos un indicador de carga
    showDialog(
      context: context,
      barrierDismissible: false, // No se puede cerrar tocando fuera
      builder: (dialogContext) => const Center(
        child: Card(
          child: Padding(
            padding: EdgeInsets.all(24.0),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                CircularProgressIndicator(),
                SizedBox(height: 16),
                Text('Enviando datos...'),
              ],
            ),
          ),
        ),
      ),
    );

    // Simulamos una petición de red de 2 segundos
    await Future.delayed(const Duration(seconds: 2));

    if (!mounted) return;
    Navigator.of(context).pop(); // Cerramos el loading

    // Mostramos éxito
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(
        content: Text('✅ Datos enviados con éxito'),
        backgroundColor: Colors.green,
        behavior: SnackBarBehavior.floating,
      ),
    );
  }

  // ==========================================================================
  // UI HELPER
  // ==========================================================================
  Widget _buildStatusUI({
    required IconData icon,
    required Color color,
    required String title,
    required String subtitle,
  }) {
    return Column(
      children: [
        Icon(icon, size: 64, color: color),
        const SizedBox(height: 16),
        Text(
          title,
          style: TextStyle(
            fontSize: 20,
            fontWeight: FontWeight.bold,
            color: color,
          ),
        ),
        const SizedBox(height: 8),
        Text(
          subtitle,
          style: const TextStyle(fontSize: 14, color: Colors.grey),
        ),
      ],
    );
  }
}
