<?php
/**
 * Plugin Name: Fuoconero Reel Worker Compatibility
 * Description: Load the WordPress file API only for Fuoconero Reel Maker worker REST requests, fixing missing wp_tempnam().
 * Version: 1.0.0
 * Author: Fuoconero
 */

defined('ABSPATH') || exit;

/**
 * WordPress does not load wp-admin/includes/file.php during normal REST requests.
 * The existing Reel Maker storage implementation calls wp_tempnam() while
 * persisting rendered outputs, which otherwise throws an undefined-function
 * fatal error. Avoid changing any unrelated REST route or site frontend.
 */
add_filter('rest_pre_dispatch', static function ($result, $server, $request) {
    if (!is_object($request) || !method_exists($request, 'get_route')) {
        return $result;
    }

    $route = $request->get_route();
    if (!is_string($route) || strpos($route, '/fuoconero-social/v2/reel-maker/render-worker/') !== 0) {
        return $result;
    }

    if (!function_exists('wp_tempnam')) {
        require_once ABSPATH . 'wp-admin/includes/file.php';
    }

    return $result;
}, 1, 3);
